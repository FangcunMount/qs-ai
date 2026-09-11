import asyncio

import pytest
from sqlalchemy import null, update

from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


def worker(ready, gateway, enabled=True):
    tx, _, _, routes, schemas = ready
    return EvaluationWorker(
        tx, gateway, routes, schemas, "worker:poll", enabled=enabled, clock=lambda: AT
    )


async def test_disabled_worker_does_not_dispatch(ready):
    gateway = Gateway(ready)
    assert not await worker(ready, gateway, False).once()
    assert gateway.calls == 0


@pytest.mark.parametrize("status", ["requested", "blocked", "canceled", "awaiting_review"])
async def test_only_started_collecting_runs_are_selected(ready, status):
    tx, run_id, *_ = ready
    run, *_ = await rows(tx, run_id)
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(progress_json={**run["progress_json"], "status": status})
        )
        await db.commit()
    gateway = Gateway(ready)
    assert not await worker(ready, gateway).once()
    assert gateway.calls == 0


@pytest.mark.parametrize("sql_null", [False, True])
async def test_preflight_then_generation_then_semantic(ready, sql_null):
    tx, run_id, *_ = ready
    run, *_ = await rows(tx, run_id)
    progress = dict(run["progress_json"])
    del progress["preflight"]
    async with tx.open() as db:
        if sql_null:
            await db.execute(
                update(evaluation_checkpoints)
                .where(evaluation_checkpoints.c.run_id == str(run_id))
                .values(checkpoint_json=null())
            )
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    gateway = Gateway(ready)
    consumer = worker(ready, gateway)
    assert await consumer.once()
    assert gateway.calls == 0
    assert await consumer.once()
    assert await consumer.once()
    assert gateway.calls == 2


async def test_other_consumer_does_not_resend_active_dispatch(ready):
    entered, finish = asyncio.Event(), asyncio.Event()

    class SlowGateway(Gateway):
        async def generate_messages(self, *args):
            entered.set()
            await finish.wait()
            return await super().generate_messages(*args)

    gateway = SlowGateway(ready)
    consumer = worker(ready, gateway)
    task = asyncio.create_task(consumer.once())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        another = worker(ready, gateway)
        assert not await another.once()
    finally:
        finish.set()
        await task
    assert gateway.calls == 1
