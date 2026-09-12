"""Reopened review rounds use real transactions and synthetic semantic judgments."""

import asyncio
import copy
import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.quality_gates import SCORE_NAMES
from qs_ai.domain.evaluation.review import (
    CONTRADICTION_POLICY,
    CandidateHumanReview,
    SemanticContradictionReview,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_reopening import reopen
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_finalization import commit, reviewed
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


@pytest.fixture
def eligible_semantics(monkeypatch):
    from tests.integration import test_semantic_completions as fixtures

    original = fixtures.case_evidence

    def judgments(case_id):
        assertions, raw = original(case_id)
        body = json.loads(raw)
        for decision in body["decisions"]:
            failed = (
                case_id == "PROMPT-EVAL-001"
                and decision["type"] == "forbidden_claims_absent"
                and decision["scope"] == "default"
            )
            decision["status"] = "failed" if failed else "passed"
        body["scores"] = dict.fromkeys(SCORE_NAMES, 5)
        return assertions, json.dumps(body).encode()

    monkeypatch.setattr(fixtures, "case_evidence", judgments)


@pytest.fixture
async def rejected_round(eligible_semantics, reviewable):
    tx, scope, _, value = reviewable
    version = await reviewed(reviewable)
    final = await commit(reviewable, version=version)
    return tx, scope, final, value.reviewed_at + timedelta(seconds=2)


async def open_round(context, **changes):
    tx, scope, final, at = context
    args = dict(
        scope=scope, expected_version=final.version, reason="复核语义判断分歧", at=at, confirm=True
    )
    args.update(changes)
    return await MySQLEvaluationManagement(tx).reopen(**args)


async def test_reopen_preserves_old_round_and_dual_review_can_approve(rejected_round):
    tx, scope, final, at = rejected_round
    original = await outputs(tx, scope.run_id)
    store = MySQLEvaluationManagement(tx)
    reopened = await open_round(rejected_round)
    assert reopened.status == "awaiting_review" and reopened.version == final.version + 1
    assert reopened.finalization_json == ""
    history = json.loads(reopened.reopenings_json)
    assert len(history) == 1
    entry = history[0]
    assert entry["previous_finalization"] == json.loads(final.finalization_json)
    assert entry["previous_reviews"] == json.loads(final.reviews_json)
    assert entry["candidate_ids"] == [f"candidate:{i}" for i in range(1, 6)]
    assert len(json.loads(reopened.reviews_json)) == 60
    assert await store.get(scope) == reopened
    assert not dict((await store.preview_gates(scope, reopened.version, at)).gate_passes)["G5"]
    state = reopened
    for uid, role in ((42, "assessment_semantics"), (43, "safety_product")):
        actor = replace(scope, operator_user_id=uid)
        reviews = []
        for cid in entry["candidate_ids"]:
            row = next(r for r in original[0] if r["candidate_id"] == cid)
            c = row["candidate_json"]
            semantic = next(
                r for r in original[1] if r["execution_id"] == c["accepted_semantic_execution_id"]
            )
            assertion = next(
                a
                for a in c["semantic_assertions"]
                if a["type"] == "forbidden_claims_absent" and a["scope"] == "default"
            )
            note = SemanticContradictionReview(
                CONTRADICTION_POLICY,
                semantic["execution_id"],
                semantic["evidence_json"]["output_fingerprint"],
                assertion["ordinal"],
                assertion["detail"],
                row["normalized_output"].decode()[:100],
                "核对原文后确认该语义判断存在分歧",
            )
            reviews.append(
                CandidateHumanReview(cid, role, actor.actor, "approve", at, "重新核对", note)
            )
        state = await store.review(actor, state.version, tuple(reviews))
    approved = await store.finalize(scope, state.version, True, "复核后通过", at, confirm=True)
    assert approved.status == "approved" and await store.get(scope) == approved
    assert json.loads(approved.reopenings_json) == history
    candidate = await store.get_candidate(scope, "candidate:1", approved.version)
    assert json.loads(candidate.evidence_json)["semantic_adjudication"] is not None
    assert await outputs(tx, scope.run_id) == original


async def test_rollback_and_concurrent_reopening_accept_one_round(rejected_round):
    tx, scope, final, at = rejected_round
    before = await rows(tx, scope.run_id)
    async with tx.open() as db:
        await reopen(db, scope, final.version, "回滚测试", at, confirm=True)
    assert await rows(tx, scope.run_id) == before
    results = await asyncio.gather(
        open_round(rejected_round), open_round(rejected_round), return_exceptions=True
    )
    assert sum(isinstance(r, CheckpointConflict) for r in results) == 1
    after = await rows(tx, scope.run_id)
    assert len(after[0]["progress_json"]["review_reopenings"]) == 1
    with pytest.raises(CheckpointConflict):
        await open_round(rejected_round, expected_version=final.version + 1)
    assert await rows(tx, scope.run_id) == after


async def test_three_committed_rounds_keep_history_and_fourth_cannot_reopen(rejected_round):
    tx, scope, final, at = rejected_round
    store = MySQLEvaluationManagement(tx)
    original = await outputs(tx, scope.run_id)
    for count in range(1, 4):
        state = await open_round((tx, scope, final, at))
        history = json.loads(state.reopenings_json)
        assert len(history) == count
        # Plain approvals do not overturn a semantic assertion; a further rejection
        # remains possible, but each new signature must belong to this round.
        for uid, role in ((42, "assessment_semantics"), (43, "safety_product")):
            actor = replace(scope, operator_user_id=uid)
            reviews = tuple(
                CandidateHumanReview(cid, role, actor.actor, "approve", at, "重新审阅原文")
                for cid in history[-1]["candidate_ids"]
            )
            if uid == 42:
                before = await rows(tx, scope.run_id)
                with pytest.raises(ValueError):
                    await store.review(
                        actor,
                        state.version,
                        tuple(replace(r, reviewed_at=at - timedelta(seconds=1)) for r in reviews),
                    )
                assert await rows(tx, scope.run_id) == before
            state = await store.review(actor, state.version, reviews)
        final = await store.finalize(
            scope, state.version, False, "语义分歧仍未解决", at, confirm=True
        )
        assert final.status == "rejected" and await store.get(scope) == final
        assert json.loads(final.reopenings_json) == history
        at += timedelta(seconds=1)
    before = await rows(tx, scope.run_id)
    with pytest.raises(ValueError):
        await open_round((tx, scope, final, at))
    assert await rows(tx, scope.run_id) == before
    assert await outputs(tx, scope.run_id) == original


@pytest.mark.parametrize("case", ["org", "version", "confirm", "reason", "early"])
async def test_invalid_reopening_does_not_mutate(rejected_round, case):
    tx, scope, final, at = rejected_round
    changes = {}
    if case == "org":
        changes.update(scope=replace(scope, organization_id=2), expected_version=final.version + 10)
    elif case == "version":
        changes["expected_version"] = final.version - 1
    elif case == "confirm":
        changes["confirm"] = False
    elif case == "reason":
        changes["reason"] = " "
    else:
        changes["at"] = at - timedelta(seconds=2)
    before = await rows(tx, scope.run_id)
    with pytest.raises(
        NotFound if case == "org" else CheckpointConflict if case == "version" else ValueError
    ):
        await open_round(rejected_round, **changes)
    assert await rows(tx, scope.run_id) == before


@pytest.mark.parametrize(
    "field",
    ["gate", "review", "ids", "boundary", "version", "unaffected", "new_time", "empty", "missing"],
)
async def test_every_read_revalidates_archived_round_and_current_signatures(rejected_round, field):
    tx, scope, _, at = rejected_round
    reopened = await open_round(rejected_round)
    run, _, _ = await rows(tx, scope.run_id)
    progress = copy.deepcopy(run["progress_json"])
    entry = progress["review_reopenings"][0]
    if field == "gate":
        entry["previous_finalization"]["gate_result"]["metrics"][0]["value"] = 0
    elif field == "review":
        entry["previous_reviews"].pop()
    elif field == "ids":
        entry["candidate_ids"].pop()
    elif field == "boundary":
        entry["transition_count"] -= 1
    elif field == "version":
        entry["source_version"] -= 1
    elif field == "unaffected":
        progress["human_reviews"][0]["reason"] = "改写旧签名"
    elif field == "new_time":
        progress["human_reviews"].append(entry["previous_reviews"][0])
    elif field == "empty":
        progress["review_reopenings"] = []
    else:
        del progress["review_reopenings"]
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    store = MySQLEvaluationManagement(tx)
    for operation in (
        store.get(scope),
        store.preview_gates(scope, reopened.version, at),
        store.get_candidate(scope, "candidate:1", reopened.version),
    ):
        with pytest.raises((ValueError, CheckpointConflict)):
            await operation
