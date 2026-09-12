import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.review import (
    CONTRADICTION_POLICY,
    CandidateHumanReview,
    SemanticContradictionReview,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews, decode_reviews
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_runs,
    evaluation_semantic_completions,
)
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import complete_candidate_set
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


@pytest.fixture
async def reviewable(judge):
    tx, run_id, *_ = judge
    state = await complete_candidate_set(judge)
    run, _, _ = await rows(tx, run_id)
    closed = datetime.fromisoformat(run["progress_json"]["transitions"][-1]["at"])
    scope = ManagementScope(run_id, 1, 42)
    value = CandidateHumanReview(
        "candidate:1", "assessment_semantics", scope.actor, "approve", closed, "事实核对完成"
    )
    return tx, scope, state.version, value


async def accept(context, values=None, *, version=None, scope=None, commit=True):
    tx, original_scope, original_version, value = context
    async with tx.open() as db:
        result = await accept_reviews(
            db,
            scope or original_scope,
            original_version if version is None else version,
            (value,) if values is None else values,
        )
        if commit:
            await db.commit()
    return result


async def outputs(tx, run_id):
    async with tx.open() as db:
        return [
            list(
                (
                    await db.execute(
                        select(table)
                        .where(table.c.run_id == str(run_id))
                        .order_by(table.c.execution_id)
                    )
                ).mappings()
            )
            for table in (evaluation_generation_completions, evaluation_semantic_completions)
        ]


async def test_complete_batch_is_versioned_and_preserves_original_outputs(reviewable):
    tx, scope, version, value = reviewable
    before = await outputs(tx, scope.run_id)
    batch = tuple(replace(value, candidate_id=f"candidate:{i}") for i in range(1, 36))
    state = await accept(reviewable, batch)
    run, _, cp = await rows(tx, scope.run_id)
    assert state.version == cp["version"] == version + 1
    assert run["progress_json"]["status"] == "awaiting_review"
    assert decode_reviews(run["progress_json"]["human_reviews"]) == batch
    other = replace(scope, operator_user_id=43)
    safety = tuple(replace(r, role="safety_product", reviewer=other.actor) for r in batch)
    await accept(reviewable, safety, version=state.version, scope=other)
    run, _, _ = await rows(tx, scope.run_id)
    assert len(run["progress_json"]["human_reviews"]) == 70
    assert run["progress_json"]["status"] == "awaiting_review"
    assert await outputs(tx, scope.run_id) == before


async def test_rollback_concurrency_and_replay_do_not_partially_accept(reviewable):
    tx, scope, version, value = reviewable
    before = await rows(tx, scope.run_id)
    await accept(reviewable, commit=False)
    assert await rows(tx, scope.run_id) == before
    results = await asyncio.gather(accept(reviewable), accept(reviewable), return_exceptions=True)
    assert sum(isinstance(r, CheckpointConflict) for r in results) == 1
    accepted = await rows(tx, scope.run_id)
    assert len(accepted[0]["progress_json"]["human_reviews"]) == 1
    assert accepted[2]["version"] == version + 1
    # A new version does not authorize overwriting an already reviewed role.
    with pytest.raises(ValueError):
        await accept(reviewable, version=version + 1)
    assert await rows(tx, scope.run_id) == accepted
    # A valid new review followed by a duplicate remains an all-or-nothing batch.
    with pytest.raises(ValueError):
        await accept(
            reviewable, (replace(value, candidate_id="candidate:2"), value), version=version + 1
        )
    assert await rows(tx, scope.run_id) == accepted


@pytest.mark.parametrize("case", ["org", "actor", "stale", "early", "missing", "same_person"])
async def test_rejected_review_never_changes_run(reviewable, case):
    tx, scope, version, value = reviewable
    options = {}
    if case == "org":
        options["scope"] = replace(scope, organization_id=2)
    elif case == "actor":
        options["scope"] = replace(scope, operator_user_id=43)
    elif case == "stale":
        options["version"] = version - 1
    elif case == "early":
        options["values"] = (replace(value, reviewed_at=value.reviewed_at - timedelta(seconds=1)),)
    elif case == "missing":
        options["values"] = (value, replace(value, candidate_id="candidate:missing"))
    else:
        await accept(reviewable)
        options.update(version=version + 1, values=(replace(value, role="safety_product"),))
    before = await rows(tx, scope.run_id)
    with pytest.raises((ValueError, CheckpointConflict)):
        await accept(reviewable, **options)
    assert await rows(tx, scope.run_id) == before


async def test_closed_flag_without_terminal_evidence_is_not_reviewable(reviewable):
    tx, scope, *_ = reviewable
    async with tx.open() as db:
        await db.execute(
            update(evaluation_semantic_completions)
            .where(evaluation_semantic_completions.c.run_id == str(scope.run_id))
            .values(normalized_output=b'{"corrupt":true}')
        )
        await db.commit()
    before = await rows(tx, scope.run_id)
    with pytest.raises(CheckpointConflict):
        await accept(reviewable)
    assert await rows(tx, scope.run_id) == before


async def test_non_reviewable_status_rejects_without_mutation(reviewable):
    tx, scope, *_ = reviewable
    run, _, _ = await rows(tx, scope.run_id)
    progress = {**run["progress_json"], "status": "canceled"}
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    before = await rows(tx, scope.run_id)
    with pytest.raises(CheckpointConflict):
        await accept(reviewable)
    assert await rows(tx, scope.run_id) == before


async def test_semantic_review_roundtrip_binds_original_without_overwriting_it(reviewable):
    tx, scope, _, value = reviewable
    before = await outputs(tx, scope.run_id)
    generation = next(r for r in before[0] if r["candidate_id"] == value.candidate_id)
    stored = generation["candidate_json"]
    semantic = next(
        r for r in before[1] if r["execution_id"] == stored["accepted_semantic_execution_id"]
    )
    assertion = next(
        a
        for a in stored["assertions"]
        if a["type"] == "forbidden_claims_absent" and a["scope"] == "default"
    )
    note = SemanticContradictionReview(
        CONTRADICTION_POLICY,
        semantic["execution_id"],
        semantic["evidence_json"]["output_fingerprint"],
        assertion["ordinal"],
        assertion["detail"],
        generation["normalized_output"].decode()[:50],
        "核对候选原文后记录判断分歧",
    )
    audited = replace(value, semantic_review=note)
    await accept(reviewable, (audited,))
    run, _, _ = await rows(tx, scope.run_id)
    assert decode_reviews(run["progress_json"]["human_reviews"]) == (audited,)
    assert await outputs(tx, scope.run_id) == before


async def test_management_returns_committed_reviews_and_supports_scoped_readback(reviewable):
    tx, scope, version, value = reviewable
    store = MySQLEvaluationManagement(tx)
    accepted = await store.review(scope, version, (value,))
    assert accepted.version == version + 1 and accepted.status == "awaiting_review"
    assert decode_reviews(json.loads(accepted.reviews_json)) == (value,)
    assert await store.get(scope) == accepted
    with pytest.raises(CheckpointConflict):
        await store.review(scope, version, (value,))
    assert await store.get(scope) == accepted
    other_org = replace(scope, organization_id=2)
    with pytest.raises(NotFound):
        await store.review(other_org, accepted.version, (value,))
    with pytest.raises(NotFound):
        await store.get(other_org)
