"""Real MySQL transactions and original ledgers; model/reviewer inputs are synthetic."""

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
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import finalize
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_reviews import accept, outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


async def reviewed(context):
    _, scope, _, value = context
    batch = tuple(replace(value, candidate_id=f"candidate:{i}") for i in range(1, 36))
    state = await accept(context, batch)
    safety_scope = replace(scope, operator_user_id=43)
    safety = tuple(replace(v, role="safety_product", reviewer=safety_scope.actor) for v in batch)
    state = await accept(context, safety, version=state.version, scope=safety_scope)
    return state.version


async def commit(context, *, version=None, passed=False, **changes):
    tx, scope, original, value = context
    args = dict(
        scope=scope,
        expected_version=version or original,
        expected_passed=passed,
        reason="核对完整门槛和评审",
        at=value.reviewed_at + timedelta(seconds=1),
        confirm=True,
    )
    args.update(changes)
    return await MySQLEvaluationManagement(tx).finalize(**args)


async def test_rejection_is_atomic_audited_and_readable_without_erasing_evidence(reviewable):
    tx, scope, _, value = reviewable
    version = await reviewed(reviewable)
    before = await rows(tx, scope.run_id)
    original = await outputs(tx, scope.run_id)
    store = MySQLEvaluationManagement(tx)
    # The client cannot turn a failing quality gate into an approval.
    with pytest.raises(CheckpointConflict):
        await commit(reviewable, version=version, passed=True)
    assert await rows(tx, scope.run_id) == before
    result = await commit(reviewable, version=version)
    assert result.status == "rejected" and result.version == version + 1
    receipt = json.loads(result.finalization_json)
    assert receipt["actor"] == scope.actor and receipt["source_version"] == version
    assert receipt["version"] == result.version and receipt["passed"] is False
    assert receipt["finalized_at"] == (value.reviewed_at + timedelta(seconds=1)).isoformat()
    assert receipt["gate_result"]["gate_passes"] == {
        "G1": True,
        "G2": True,
        "G3": True,
        "G4": False,
        "G5": True,
    }
    assert await store.get(scope) == result
    assert await outputs(tx, scope.run_id) == original
    after = await rows(tx, scope.run_id)
    assert after[0]["definition_json"] == before[0]["definition_json"]
    assert after[0]["progress_json"]["human_reviews"] == before[0]["progress_json"]["human_reviews"]
    assert (
        after[0]["progress_json"]["transitions"][:-1] == before[0]["progress_json"]["transitions"]
    )
    for replay_version in (version, version + 1):
        with pytest.raises(CheckpointConflict):
            await commit(reviewable, version=replay_version)
    with pytest.raises(CheckpointConflict):
        await store.review(scope, result.version, (value,))
    assert await rows(tx, scope.run_id) == after


async def test_incomplete_reviews_cannot_be_finalized_even_as_rejected(reviewable):
    tx, scope, *_ = reviewable
    before = await rows(tx, scope.run_id)
    with pytest.raises(ValueError, match="complete"):
        await commit(reviewable)
    assert await rows(tx, scope.run_id) == before


@pytest.mark.parametrize("case", ["foreign", "stale", "no_confirm", "not_bool", "reason", "early"])
async def test_invalid_finalization_never_writes(reviewable, case):
    tx, scope, _, value = reviewable
    version = await reviewed(reviewable)
    args = dict(version=version)
    if case == "foreign":
        args["scope"] = replace(scope, organization_id=2)
        args["version"] = version + 50
    elif case == "stale":
        args["version"] = version - 1
    elif case == "no_confirm":
        args["confirm"] = False
    elif case == "not_bool":
        args["passed"] = 0
    elif case == "reason":
        args["reason"] = " "
    else:
        args["at"] = value.reviewed_at - timedelta(seconds=1)
    before = await rows(tx, scope.run_id)
    expected = (
        NotFound if case == "foreign" else CheckpointConflict if case == "stale" else ValueError
    )
    with pytest.raises(expected):
        await commit(reviewable, **args)
    assert await rows(tx, scope.run_id) == before


async def test_rollback_and_concurrent_finalizations_accept_exactly_one_decision(reviewable):
    tx, scope, _, value = reviewable
    version = await reviewed(reviewable)
    before = await rows(tx, scope.run_id)
    async with tx.open() as db:
        await finalize(db, scope, version, False, "回滚验证", value.reviewed_at, confirm=True)
        # Deliberately leave without committing both the Run and checkpoint writes.
    assert await rows(tx, scope.run_id) == before
    results = await asyncio.gather(
        commit(reviewable, version=version),
        commit(reviewable, version=version),
        return_exceptions=True,
    )
    assert sum(isinstance(result, CheckpointConflict) for result in results) == 1
    assert (await rows(tx, scope.run_id))[2]["version"] == version + 1


@pytest.mark.parametrize("field", ["decision", "metric", "review", "transition", "version", "type"])
async def test_final_read_recomputes_gate_and_rejects_corrupt_audit(reviewable, field):
    tx, scope, *_ = reviewable
    version = await reviewed(reviewable)
    await commit(reviewable, version=version)
    run, _, _ = await rows(tx, scope.run_id)
    progress = copy.deepcopy(run["progress_json"])
    if field == "decision":
        progress["gate_result"]["passed"] = True
    elif field == "metric":
        progress["gate_result"]["gate_result"]["metrics"][0]["value"] = 0
    elif field == "review":
        progress["human_reviews"].pop()
    elif field == "transition":
        progress["transitions"][-1]["actor"] = "user:999"
    elif field == "version":
        progress["gate_result"]["source_version"] -= 1
    else:
        progress["gate_result"]["passed"] = 0
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    with pytest.raises((ValueError, CheckpointConflict)):
        await MySQLEvaluationManagement(tx).get(scope)


@pytest.fixture
def passing_semantics(monkeypatch):
    from tests.integration import test_semantic_completions as fixtures

    original = fixtures.case_evidence

    def passing(case_id):
        assertions, raw = original(case_id)
        result = json.loads(raw)
        for decision in result["decisions"]:
            decision.update(status="passed", detail="synthetic passing semantic receipt")
        result["scores"] = dict.fromkeys(SCORE_NAMES, 5)
        return assertions, json.dumps(result).encode()

    # Only provider test data changes; the real gate calculator and storage are used.
    monkeypatch.setattr(fixtures, "case_evidence", passing)


@pytest.fixture
async def passing_reviewable(passing_semantics, reviewable):
    return reviewable


async def test_all_gates_pass_approves_and_binds_the_immutable_release(passing_reviewable):
    tx, scope, *_ = passing_reviewable
    version = await reviewed(passing_reviewable)
    result = await commit(passing_reviewable, version=version, passed=True)
    assert result.status == "approved"
    receipt = json.loads(result.finalization_json)
    assert all(receipt["gate_result"]["gate_passes"].values())
    run, _, _ = await rows(tx, scope.run_id)
    assert (
        receipt["release_fingerprint"] == json.loads(run["definition_json"])["release_fingerprint"]
    )
    assert await MySQLEvaluationManagement(tx).get(scope) == result


async def test_finalization_waits_for_review_commit_before_establishing_snapshot(reviewable):
    tx, scope, _, value = reviewable
    batch = tuple(replace(value, candidate_id=f"candidate:{i}") for i in range(1, 36))
    state = await accept(reviewable, batch)
    other = replace(scope, operator_user_id=43)
    safety = tuple(replace(v, role="safety_product", reviewer=other.actor) for v in batch)
    state = await accept(reviewable, safety[:-1], version=state.version, scope=other)
    pending = None
    try:
        async with tx.open() as db:
            accepted = await accept_reviews(db, other, state.version, safety[-1:])
            pending = asyncio.create_task(commit(reviewable, version=accepted.version))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending), timeout=0.15)
            await db.commit()
        result = await asyncio.wait_for(pending, timeout=5)
        assert result.status == "rejected" and result.version == accepted.version + 1
        assert len(json.loads(result.reviews_json)) == 70
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
