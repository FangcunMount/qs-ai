from datetime import timedelta

import pytest
from sqlalchemy import select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.evaluation_recovery import recover_expired
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration
EXPIRY = AT + timedelta(minutes=5)


async def pending(ready, *, dispatched=False, version=3):
    tx, run_id, *_ = ready
    async with tx.open() as db:
        state = await prepare_execution(
            db, run_id, version, 1, "worker:dead", "execution:dead", "invocation:dead", AT, EXPIRY
        )
        if dispatched:
            state = await reserve_dispatch(db, run_id, state.version, "worker:dead", AT)
        await db.commit()
    return state


async def recover(ready, state, *, commit=True, **changes):
    tx, run_id, _, routes, schemas = ready
    args = dict(
        run_id=run_id,
        expected_version=state.version,
        organization_id=1,
        invocation_id="invocation:dead",
        observed_expiry=EXPIRY,
        at=EXPIRY,
        actor="system:recovery",
        routes=routes,
        schemas=schemas,
    )
    args.update(changes)
    async with tx.open() as db:
        result = await recover_expired(db, **args)
        if commit:
            await db.commit()
    return result


async def test_expired_preparation_releases_without_spending_dispatch(ready):
    state = await pending(ready)
    recovered = await recover(ready, state)
    assert recovered.checkpoint is None
    tx, run_id, *_ = ready
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "collecting"
    assert run["progress_json"]["recoveries"][0]["cause_code"] == "expired_preparation_released"
    gateway = Gateway(ready)
    await step(ready, gateway, recovered.version)
    assert gateway.calls == 1


@pytest.mark.parametrize("semantic", [False, True])
async def test_dispatched_unknown_blocks_and_preserves_audit(ready, semantic):
    version = 3
    if semantic:
        version = (await step(ready, Gateway(ready))).version
    state = await pending(ready, dispatched=True, version=version)
    recovered = await recover(ready, state)
    tx, run_id, *_ = ready
    run, _, checkpoint = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    assert checkpoint["checkpoint_json"] is None
    table = evaluation_semantic_completions if semantic else evaluation_generation_completions
    async with tx.open() as db:
        saved = (
            (
                await db.execute(
                    select(table).where(
                        table.c.run_id == str(run_id), table.c.invocation_id == "invocation:dead"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert saved["evidence_json"]["status"] == "result_unknown"
    assert saved["evidence_json"]["receipt"] is None
    assert saved["raw_output"] == b""
    assert run["progress_json"]["recoveries"][0]["previous_owner"] == "worker:dead"
    with pytest.raises(CheckpointConflict):
        await recover(ready, state)
    with pytest.raises(CheckpointConflict):
        await step(ready, Gateway(ready), recovered.version)


@pytest.mark.parametrize(
    "changes",
    [
        {"organization_id": 2},
        {"invocation_id": "wrong"},
        {"observed_expiry": EXPIRY + timedelta(seconds=1)},
        {"at": AT},
        {"expected_version": 99},
    ],
)
async def test_stale_or_wrong_recovery_cannot_mutate(ready, changes):
    state = await pending(ready, dispatched=True)
    with pytest.raises(CheckpointConflict):
        await recover(ready, state, **changes)
    tx, run_id, *_ = ready
    assert (await rows(tx, run_id))[2]["version"] == state.version


async def test_recovery_rolls_back_terminal_and_audit_together(ready):
    state = await pending(ready, dispatched=True)
    await recover(ready, state, commit=False)
    tx, run_id, *_ = ready
    run, _, checkpoint = await rows(tx, run_id)
    assert checkpoint["version"] == state.version
    assert run["progress_json"]["status"] == "collecting"
    assert "recoveries" not in run["progress_json"]
    await recover(ready, state)


async def test_concurrent_recovery_accepts_only_once(ready):
    import asyncio

    state = await pending(ready, dispatched=True)
    results = await asyncio.gather(
        recover(ready, state), recover(ready, state), return_exceptions=True
    )
    assert sum(isinstance(result, CheckpointConflict) for result in results) == 1
    tx, run_id, *_ = ready
    run, _, checkpoint = await rows(tx, run_id)
    assert checkpoint["version"] == state.version + 1
    assert len(run["progress_json"]["recoveries"]) == 1
