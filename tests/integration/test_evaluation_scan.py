from datetime import timedelta

import pytest

from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor, recover_next
from tests.integration.test_evaluation_recovery import EXPIRY, pending
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_worker import worker

pytestmark = pytest.mark.integration


async def scan(ready, cursor, at=EXPIRY):
    tx, _, _, routes, schemas = ready
    return await recover_next(tx, routes, schemas, "system:scan", at, cursor, batch_size=1)


async def test_keyset_wrap_revisits_expired_run(ready):
    await pending(ready, dispatched=True)
    cursor = RecoveryCursor("ffffffff-ffff-ffff-ffff-ffffffffffff")
    assert not await scan(ready, cursor)
    assert cursor.after_run_id == ""
    assert await scan(ready, cursor)
    tx, run_id, *_ = ready
    assert (await rows(tx, run_id))[0]["progress_json"]["status"] == "blocked"
    assert not await scan(ready, cursor)


async def test_unexpired_checkpoint_not_recovered_then_revisited(ready):
    state = await pending(ready, dispatched=True)
    cursor = RecoveryCursor()
    assert not await scan(ready, cursor, AT)
    tx, run_id, *_ = ready
    assert (await rows(tx, run_id))[2]["version"] == state.version
    assert not await scan(ready, cursor, EXPIRY)  # end of page wraps cursor
    assert await scan(ready, cursor, EXPIRY)


async def test_worker_recovers_dispatch_without_calling_model(ready):
    await pending(ready, dispatched=True)
    gateway = Gateway(ready)
    consumer = worker(ready, gateway)
    consumer.clock = lambda: EXPIRY + timedelta(seconds=1)
    assert await consumer.once()
    assert not await consumer.once()
    assert gateway.calls == 0


async def test_worker_releases_preparation_then_executes_next_poll(ready):
    await pending(ready)
    gateway = Gateway(ready)
    consumer = worker(ready, gateway)
    consumer.clock = lambda: EXPIRY
    assert await consumer.once()
    assert gateway.calls == 0
    assert await consumer.once()
    assert gateway.calls == 1


async def test_disabled_worker_does_not_recover(ready):
    state = await pending(ready, dispatched=True)
    consumer = worker(ready, Gateway(ready), enabled=False)
    consumer.clock = lambda: EXPIRY
    assert not await consumer.once()
    tx, run_id, *_ = ready
    assert (await rows(tx, run_id))[2]["version"] == state.version


@pytest.mark.parametrize("damaged", [False, True])
async def test_bounded_scan_advances_past_earlier_unavailable_record(ready, damaged):
    from sqlalchemy import delete, insert

    from qs_ai.infrastructure.persistence.mysql.schema import (
        evaluation_checkpoints,
        evaluation_runs,
    )

    await pending(ready, dispatched=True)
    tx, run_id, *_ = ready
    _, _, saved = await rows(tx, run_id)
    earlier_id = "00000000-0000-0000-0000-000000000001"
    assert earlier_id < str(run_id)
    value = dict(saved["checkpoint_json"])
    value["lease_expires_at"] = (EXPIRY + timedelta(days=1)).isoformat()
    if damaged:
        value = {"invalid": True}
    async with tx.open() as db:
        await db.execute(
            insert(evaluation_runs).values(
                run_id=earlier_id,
                organization_id=1,
                requested_by="fixture:scan",
                definition_json="{}",
                progress_json={"status": "collecting"},
            )
        )
        await db.execute(
            insert(evaluation_checkpoints).values(
                run_id=earlier_id,
                version=1,
                checkpoint_json=value,
            )
        )
        await db.commit()
    try:
        cursor = RecoveryCursor()
        if damaged:
            with pytest.raises((KeyError, TypeError)):
                await scan(ready, cursor)
        else:
            assert not await scan(ready, cursor)
        assert cursor.after_run_id == earlier_id
        assert await scan(ready, cursor)
        assert (await rows(tx, run_id))[0]["progress_json"]["status"] == "blocked"
    finally:
        async with tx.open() as db:
            for table in (evaluation_checkpoints, evaluation_runs):
                await db.execute(delete(table).where(table.c.run_id == earlier_id))
            await db.commit()
