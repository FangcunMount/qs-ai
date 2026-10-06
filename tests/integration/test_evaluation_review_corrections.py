"""Real transactions with synthetic candidates; no network model calls or production writes."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.review_correction import review_fingerprint
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_finalization import passing_semantics as passing_semantics
from tests.integration.test_evaluation_reopening import eligible_semantics as eligible_semantics
from tests.integration.test_evaluation_reviews import accept, outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


@pytest.fixture
async def signatures(passing_semantics, reviewable):
    tx, scope, _, value = reviewable
    originals = tuple(
        replace(value, candidate_id=f"candidate:{i}", decision="reject" if i == 1 else "approve")
        for i in range(1, 36)
    )
    state = await accept(reviewable, originals)
    other = replace(scope, operator_user_id=43)
    safety = tuple(
        replace(v, role="safety_product", reviewer=other.actor, decision="approve")
        for v in originals
    )
    state = await accept(reviewable, safety, version=state.version, scope=other)
    store = MySQLEvaluationManagement(tx)
    detail = await store.get_candidate(scope, value.candidate_id, state.version)
    # Receipt-bound output identity, never a hash of a UI-reformatted document.
    candidate = json.loads(detail.evidence_json)["candidate"]
    fingerprint = candidate["normalized_output_fingerprint"]
    return tx, scope, state.version, originals[0], fingerprint, store


def intent(context, **changes):
    _, scope, version, original, fingerprint, _ = context
    value = replace(
        original,
        decision="approve",
        reason="标题可改进；正文与原引用一致",
        reviewed_at=original.reviewed_at + timedelta(seconds=1),
    )
    args = dict(
        scope=scope,
        expected_version=version,
        command_id=str(uuid4()),
        previous_review_fingerprint=review_fingerprint(original),
        candidate_output_fingerprint=fingerprint,
        value=value,
    )
    return {**args, **changes}


async def test_gate_recalculation_replay_and_finalization_preserve_originals(signatures):
    tx, scope, version, original, _, store = signatures
    args = intent(signatures)
    before = await rows(tx, scope.run_id)
    ledgers = await outputs(tx, scope.run_id)
    gate = await store.preview_gates(scope, version, args["value"].reviewed_at)
    assert dict(gate.gate_passes)["G5"] is False
    after = await store.correct_review(**args)
    assert after.version == version + 1 and after.status == "awaiting_review"
    assert json.loads(after.original_reviews_json) == before[0]["progress_json"]["human_reviews"]
    assert json.loads(after.reviews_json)[0]["decision"] == "approve"
    assert len(json.loads(after.review_corrections_json)) == 1
    assert await store.correct_review(**args) == after
    final_at = args["value"].reviewed_at + timedelta(seconds=1)
    preview = await store.preview_gates(scope, after.version, final_at)
    assert all(dict(preview.gate_passes).values())
    final = await store.finalize(scope, after.version, True, "核对完整审计", final_at, confirm=True)
    assert final.status == "approved"
    assert await store.correct_review(**args) == final  # Replay is not a new correction.
    with pytest.raises(CheckpointConflict):
        await store.correct_review(**intent(signatures, expected_version=final.version))
    assert await outputs(tx, scope.run_id) == ledgers
    assert (await rows(tx, scope.run_id))[0]["definition_json"] == before[0]["definition_json"]
    # A forged amendment must not be trusted by readback or final publication validation.
    async with tx.open() as db:
        run, _, _ = await rows(tx, scope.run_id)
        progress = json.loads(json.dumps(run["progress_json"]))
        progress["review_corrections"][0]["previous_review"]["reason"] = "tampered"
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    with pytest.raises(ValueError):
        await store.get(scope)


async def test_parallel_replay_is_one_write_and_different_intent_conflicts(signatures):
    tx, scope, version, _, _, store = signatures
    args = intent(signatures)
    result = await asyncio.gather(store.correct_review(**args), store.correct_review(**args))
    assert result[0] == result[1] and result[0].version == version + 1
    for changed in [
        dict(value=replace(args["value"], reason="different")),
        dict(
            scope=replace(scope, operator_user_id=43),
            value=replace(args["value"], reviewer="user:43"),
        ),
    ]:
        with pytest.raises(CheckpointConflict):
            await store.correct_review(**{**args, **changed})
    assert len((await rows(tx, scope.run_id))[0]["progress_json"]["review_corrections"]) == 1


@pytest.mark.parametrize("case", ["org", "owner", "stale", "fingerprint", "body", "missing"])
async def test_invalid_correction_never_writes(signatures, case):
    tx, scope, _, _, _, store = signatures
    args = intent(signatures)
    if case == "org":
        args["scope"] = replace(scope, organization_id=2)
    if case == "owner":
        args["scope"] = replace(scope, operator_user_id=43)
        args["value"] = replace(args["value"], reviewer="user:43")
    if case == "stale":
        args["expected_version"] -= 1
    if case == "fingerprint":
        args["previous_review_fingerprint"] = "sha256:" + "0" * 64
    if case == "body":
        args["candidate_output_fingerprint"] = "sha256:" + "0" * 64
    if case == "missing":
        args["value"] = replace(args["value"], candidate_id="candidate:missing")
    before = await rows(tx, scope.run_id)
    with pytest.raises((ValueError, CheckpointConflict, NotFound)):
        await store.correct_review(**args)
    assert await rows(tx, scope.run_id) == before


async def test_failed_transaction_and_competing_new_commands_do_not_overwrite(signatures):
    from qs_ai.infrastructure.persistence.mysql.evaluation_review_corrections import correct_review

    tx, scope, version, _, _, store = signatures
    args = intent(signatures)
    before = await rows(tx, scope.run_id)
    async with tx.open() as db:
        await correct_review(db, **args)
        # Leave without commit, simulating a failure before the command receipt commits.
    assert await rows(tx, scope.run_id) == before
    competitor = {**args, "command_id": str(uuid4())}
    results = await asyncio.gather(
        store.correct_review(**args), store.correct_review(**competitor), return_exceptions=True
    )
    assert sum(isinstance(result, CheckpointConflict) for result in results) == 1
    assert (await rows(tx, scope.run_id))[2]["version"] == version + 1


async def test_semantic_reopening_retains_correction_audit_and_command_replay(
    eligible_semantics, reviewable
):
    tx, scope, _, value = reviewable
    originals = tuple(
        replace(value, candidate_id=f"candidate:{i}", decision="reject" if i == 1 else "approve")
        for i in range(1, 36)
    )
    state = await accept(reviewable, originals)
    other = replace(scope, operator_user_id=43)
    safety = tuple(
        replace(r, role="safety_product", reviewer=other.actor, decision="approve")
        for r in originals
    )
    state = await accept(reviewable, safety, version=state.version, scope=other)
    store = MySQLEvaluationManagement(tx)
    detail = await store.get_candidate(scope, value.candidate_id, state.version)
    context = (
        tx,
        scope,
        state.version,
        originals[0],
        json.loads(detail.evidence_json)["candidate"]["normalized_output_fingerprint"],
        store,
    )
    args = intent(context)
    corrected = await store.correct_review(**args)
    at = args["value"].reviewed_at + timedelta(seconds=1)
    rejected = await store.finalize(
        scope, corrected.version, False, "裁判争议仍需复核", at, confirm=True
    )
    assert rejected.can_reopen_review
    opened = await store.reopen(
        scope, rejected.version, "按原规则重开争议审核", at + timedelta(seconds=1), confirm=True
    )
    history = json.loads(opened.reopenings_json)
    assert history[0]["previous_original_reviews"][0]["decision"] == "reject"
    assert history[0]["previous_reviews"][0]["decision"] == "approve"
    assert history[0]["previous_review_corrections"][0]["command_id"] == args["command_id"]
    assert await store.correct_review(**args) == opened
