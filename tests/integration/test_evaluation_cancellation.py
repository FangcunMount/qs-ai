"""MySQL cancellation, preserved evidence and dispatch/CAS races; no real models."""

import asyncio
import copy
import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import select, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import (
    cancel,
    finish_cancellation,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import (
    complete_preflight,
    execute_preflight,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches, evaluation_runs
from tests.integration.test_evaluation_completions import accept, stored
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_recovery import EXPIRY, pending, recover
from tests.integration.test_evaluation_reopening import eligible_semantics as eligible_semantics
from tests.integration.test_evaluation_reopening import open_round
from tests.integration.test_evaluation_reopening import rejected_round as rejected_round
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_start import AT
from tests.integration.test_evaluation_start import requested as requested
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


async def test_requested_cancellation_is_atomic_audited_and_competes_with_start(requested):
    tx, scope, store = requested
    before = await rows(tx, scope.run_id)
    async with tx.open() as db:
        await cancel(db, scope, 1, "事务回滚", AT, discard=False, confirm=True)
    assert await rows(tx, scope.run_id) == before
    result = await store.cancel(scope, 1, "取消未启动任务", AT, discard=False, confirm=True)
    assert (result.status, result.version) == ("canceled", 2)
    assert await store.get(scope) == result
    record = json.loads(result.cancellation_json)
    assert (record["actor"], record["source_version"], record["discard"]) == (scope.actor, 1, False)
    after = await rows(tx, scope.run_id)
    assert after[0]["definition_json"] == before[0]["definition_json"]
    assert after[1] == before[1] and after[2]["checkpoint_json"] is None
    for version in (1, 2):
        with pytest.raises(CheckpointConflict):
            await store.start(scope, version, "迟到的启动", AT, confirm=True)
        with pytest.raises(CheckpointConflict):
            await store.cancel(scope, version, "重复取消", AT, discard=False, confirm=True)
    assert await rows(tx, scope.run_id) == after


@pytest.mark.parametrize(
    "invalid", ["scope", "stale", "confirm", "discard", "reason", "early", "bool_version"]
)
async def test_invalid_cancel_does_not_write(requested, invalid):
    tx, scope, store = requested
    args = dict(scope=scope, expected_version=1, reason="停止", at=AT, discard=False, confirm=True)
    if invalid == "scope":
        args["scope"] = replace(scope, organization_id=2)
    elif invalid == "stale":
        args["expected_version"] = 5
    elif invalid == "confirm":
        args["confirm"] = False
    elif invalid == "discard":
        args["discard"] = True
    elif invalid == "reason":
        args["reason"] = " "
    elif invalid == "early":
        args["at"] = AT - timedelta(seconds=1)
    else:
        args["expected_version"] = True
    before = await rows(tx, scope.run_id)
    with pytest.raises((ValueError, CheckpointConflict, NotFound)):
        await store.cancel(**args)
    assert await rows(tx, scope.run_id) == before


async def prepared(requested):
    tx, scope, store = requested
    await store.start(scope, 1, "开始", AT, confirm=True)
    async with tx.open() as db:
        await execute_preflight(db, scope.run_id, 2, scope.organization_id, AT)
        state = await prepare_execution(
            db,
            scope.run_id,
            3,
            scope.organization_id,
            "worker:1",
            "execution:1",
            "invocation:1",
            AT,
            AT + timedelta(minutes=1),
        )
        await db.commit()
    return state


@pytest.mark.parametrize("blocked", [False, True])
async def test_cancel_without_checkpoint_preserves_preflight_and_blocks_late_work(
    requested, blocked
):
    tx, scope, store = requested
    state = await store.start(scope, 1, "开始", AT, confirm=True)
    if blocked:
        run, *_ = await rows(tx, scope.run_id)
        case_id = json.loads(run["definition_json"])["preflight"]["case_id"]
        async with tx.open() as db:
            state = await complete_preflight(
                db,
                scope.run_id,
                state.version,
                scope.organization_id,
                PreflightEvidence(
                    case_id,
                    "failed",
                    AT,
                    0,
                    "preflight_failed",
                    (
                        AssertionReceipt(
                            "rejection_reason",
                            "default",
                            1,
                            True,
                            "deterministic",
                            "failed",
                            "预检未通过",
                        ),
                    ),
                ),
            )
            await db.commit()
    before = await rows(tx, scope.run_id)
    result = await store.cancel(scope, state.version, "停止任务", AT, discard=False, confirm=True)
    record = json.loads(result.cancellation_json)
    assert record["source_status"] == ("blocked" if blocked else "collecting")
    assert record["execution_id"] == record["invocation_id"] == ""
    after = await rows(tx, scope.run_id)
    assert after[0]["progress_json"].get("preflight") == before[0]["progress_json"].get("preflight")
    for version in (state.version, result.version):
        async with tx.open() as db:
            with pytest.raises(CheckpointConflict):
                await execute_preflight(db, scope.run_id, version, scope.organization_id, AT)
    assert await rows(tx, scope.run_id) == after
    assert await store.get(scope) == result


async def test_dispatched_call_must_finish_before_cancel_and_keeps_accepted_output(dispatched):
    tx, run_id, value, *_ = dispatched
    scope = ManagementScope(run_id, 1, 42)
    store = MySQLEvaluationManagement(tx)
    pending_cancel = await store.cancel(
        scope, 5, "调用已发送", value.finished_at, discard=False, confirm=True
    )
    assert pending_cancel.status == "collecting"
    assert pending_cancel.cancel_draining and pending_cancel.cancellation_json == ""
    assert json.loads(pending_cancel.cancel_request_json)["status"] == "cancel_requested"
    async with tx.open() as db:
        await accept(db, dispatched)
        await db.commit()
    evidence = await stored(tx, run_id)
    async with tx.open() as db:
        assert await finish_cancellation(db, scope, value.finished_at)
        await db.commit()
    result = await store.get(scope)
    assert result.status == "canceled" and await store.get(scope) == result
    assert await stored(tx, run_id) == evidence
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await accept(db, dispatched, expected_version=result.version)
    assert await stored(tx, run_id) == evidence


async def test_prepared_cancellation_preserves_checkpoint_and_denies_late_dispatch(requested):
    tx, scope, store = requested
    state = await prepared(requested)
    before = await rows(tx, scope.run_id)
    result = await store.cancel(
        scope, state.version, "停止未发送调用", AT, discard=False, confirm=True
    )
    after = await rows(tx, scope.run_id)
    assert after[0]["progress_json"]["canceled_checkpoint"] == before[2]["checkpoint_json"]
    assert json.loads(result.cancellation_json)["execution_id"] == "execution:1"
    assert after[2]["checkpoint_json"] is None
    assert await store.get(scope) == result
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await reserve_dispatch(db, scope.run_id, state.version, "worker:1", AT)
        assert (
            await db.execute(
                select(evaluation_dispatches).where(
                    evaluation_dispatches.c.run_id == str(scope.run_id)
                )
            )
        ).first() is None


async def test_dispatch_and_cancellation_cannot_both_win(requested):
    tx, scope, store = requested
    state = await prepared(requested)

    async def dispatch():
        async with tx.open() as db:
            value = await reserve_dispatch(db, scope.run_id, state.version, "worker:1", AT)
            await db.commit()
            return value

    results = await asyncio.gather(
        dispatch(),
        store.cancel(scope, state.version, "停止", AT, discard=False, confirm=True),
        return_exceptions=True,
    )
    assert sum(isinstance(r, CheckpointConflict) for r in results) == 1
    run, _, checkpoint = await rows(tx, scope.run_id)
    assert checkpoint["version"] == state.version + 1
    assert (run["progress_json"]["status"] == "canceled") == (checkpoint["checkpoint_json"] is None)
    if checkpoint["checkpoint_json"] is not None:
        result = await store.cancel(
            scope, checkpoint["version"], "停止并排空已发送调用", AT, discard=False, confirm=True
        )
        assert result.status == "collecting"
        assert json.loads(result.cancel_request_json)["status"] == "cancel_requested"


async def test_unknown_calls_require_the_original_resolution_path(ready):
    tx, run_id, *_ = ready
    state = await pending(ready, dispatched=True)
    state = await recover(ready, state)
    before = await rows(tx, run_id)
    scope = ManagementScope(run_id, 1, 42)
    result = await MySQLEvaluationManagement(tx).cancel(
        scope, state.version, "不能绕开未知调用核对", EXPIRY, discard=False, confirm=True
    )
    assert result.status == "blocked"
    assert result.unresolved_result_unknown_count == 1
    after = await rows(tx, run_id)
    assert after[0]["progress_json"].get("result_unknown_resolutions", []) == before[0][
        "progress_json"
    ].get("result_unknown_resolutions", [])
    async with tx.open() as db:
        assert not await finish_cancellation(db, scope, EXPIRY)


async def test_discard_keeps_reopened_reviews_and_all_original_outputs(rejected_round):
    tx, scope, final, at = rejected_round
    store = MySQLEvaluationManagement(tx)
    with pytest.raises(CheckpointConflict):
        await store.cancel(scope, final.version, "终态不能取消", at, discard=True, confirm=True)
    reopened = await open_round(rejected_round)
    evidence = await outputs(tx, scope.run_id)
    candidate = await store.get_candidate(scope, "candidate:1", reopened.version)
    with pytest.raises(CheckpointConflict):
        await store.cancel(
            scope, reopened.version, "需明确放弃评审", at, discard=False, confirm=True
        )
    result = await store.cancel(
        scope, reopened.version, "放弃本轮评审", at, discard=True, confirm=True
    )
    assert (result.reopenings_json, result.reviews_json) == (
        reopened.reopenings_json,
        reopened.reviews_json,
    )
    assert result.finalization_json == "" and result.status == "canceled"
    assert await store.get(scope) == result
    retained = await store.get_candidate(scope, "candidate:1", result.version)
    assert replace(retained, version=candidate.version) == candidate
    assert await outputs(tx, scope.run_id) == evidence


@pytest.mark.parametrize(
    "field", ["actor", "source_version", "release_fingerprint", "discard", "execution_id"]
)
async def test_read_rejects_corrupted_cancellation_receipt(requested, field):
    tx, scope, store = requested
    await store.cancel(scope, 1, "停止", AT, discard=False, confirm=True)
    run, *_ = await rows(tx, scope.run_id)
    progress = copy.deepcopy(run["progress_json"])
    progress["cancellation"][field] = "wrong"
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    with pytest.raises((ValueError, CheckpointConflict)):
        await store.get(scope)
