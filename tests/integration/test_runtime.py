import asyncio
import json
import os
import sys
from uuid import uuid4

import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.mysql.asyncmy import AsyncMySaver
from sqlalchemy import text

from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.leases import CheckpointLeases, LeaseLost
from qs_ai.infrastructure.workflows.langgraph.checkpoints import guarded_saver
from tests.probes.clarification import build_demo

pytestmark = pytest.mark.integration


@pytest.fixture
async def runtime():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL and alembic upgrade head")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    thread_id = f"p0-{uuid4()}"
    try:
        async with AsyncMySaver.from_conn_string(dsn) as saver:
            await saver.setup()
        yield dsn, transactions, CheckpointLeases(transactions), thread_id
    finally:
        async with AsyncMySaver.from_conn_string(dsn) as saver:
            await saver.adelete_thread(thread_id)
        async with transactions.open() as session:
            await session.execute(
                text("DELETE FROM checkpoint_leases WHERE thread_id = :id"), {"id": thread_id}
            )
            await session.commit()
        await database.close()


async def expire(transactions, thread_id):
    async with transactions.open() as session:
        await session.execute(
            text(
                "UPDATE checkpoint_leases SET expires_at = "
                "TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6)) WHERE thread_id = :id"
            ),
            {"id": thread_id},
        )
        await session.commit()


async def test_transaction_isolation_and_rollback(runtime):
    _, transactions, _, thread_id = runtime
    with pytest.raises(ValueError):
        async with transactions.open() as first:
            async with transactions.open() as second:
                assert first is not second
            await first.execute(
                text("INSERT INTO checkpoint_leases VALUES (:id, 1, UTC_TIMESTAMP(6))"),
                {"id": thread_id},
            )
            raise ValueError("rollback")
    async with transactions.open() as session:
        count = await session.scalar(
            text("SELECT COUNT(*) FROM checkpoint_leases WHERE thread_id = :id"), {"id": thread_id}
        )
        assert count == 0


async def test_takeover_rejects_every_stale_write(runtime):
    dsn, transactions, leases, thread_id = runtime
    first = await leases.acquire(thread_id)
    assert first is not None
    assert await leases.acquire(thread_id) is None
    config = {"configurable": {"thread_id": thread_id}}
    async with guarded_saver(dsn, first) as old:
        paused = await build_demo(old).ainvoke({"question": "Who answered?"}, config)
        assert paused["__interrupt__"]
        before = await old.aget_tuple(config)
        await expire(transactions, thread_id)
        candidates = await asyncio.gather(leases.acquire(thread_id), leases.acquire(thread_id))
        assert sum(candidate is not None for candidate in candidates) == 1
        current = next(candidate for candidate in candidates if candidate is not None)
        assert current.fence > first.fence
        await leases.release(first)  # A stale release cannot release the current owner's lease.
        assert await leases.acquire(thread_id) is None
        with pytest.raises(LeaseLost):
            await old.aput(before.config, empty_checkpoint(), {}, {})
        with pytest.raises(LeaseLost):
            await old.aput_writes(before.config, [("answer", "stale")], "old-task")
        with pytest.raises(LeaseLost):
            await old.adelete_thread(thread_id)
        assert await old.aget_tuple(config) == before
        async with guarded_saver(dsn, current) as active:
            with pytest.raises(LeaseLost):
                await active.adelete_thread("another-thread")
            await active.aput_writes(before.config, [("answer", "valid")], "new-task")
            after = await active.aget_tuple(config)
            assert ("new-task", "answer", "valid") in after.pending_writes


async def test_expiry_during_write_rolls_back(runtime):
    dsn, _, leases, thread_id = runtime
    lease = await leases.acquire(thread_id, ttl_seconds=1)
    async with guarded_saver(dsn, lease) as saver:
        original_check = saver._check_fence
        checks = 0

        async def delayed_check(cursor):
            nonlocal checks
            await original_check(cursor)
            checks += 1
            if checks == 1:
                await asyncio.sleep(1.1)

        saver._check_fence = delayed_check
        config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
        with pytest.raises(LeaseLost):
            await saver.aput(config, empty_checkpoint(), {}, {})
        assert checks == 1  # The second check raised before commit.
        assert await saver.aget_tuple(config) is None


async def test_checkpoint_recovers_after_process_kill(runtime):
    dsn, transactions, leases, thread_id = runtime
    lease = await leases.acquire(thread_id)
    args = [sys.executable, "-m", "tests.probes.checkpoint_process"]
    first = await asyncio.create_subprocess_exec(
        *args,
        "pause",
        thread_id,
        str(lease.fence),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert await asyncio.wait_for(first.stdout.readline(), 20) == b"PAUSED\n"
    finally:
        if first.returncode is None:
            first.kill()
        await first.communicate()
    assert first.returncode != 0
    await expire(transactions, thread_id)
    new_lease = await leases.acquire(thread_id)
    second = await asyncio.create_subprocess_exec(
        *args,
        "resume",
        thread_id,
        str(new_lease.fence),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, error = await asyncio.wait_for(second.communicate(), 20)
        assert second.returncode == 0, error.decode()
        result = json.loads(output)
        assert result["answer"] == "Father"
        assert result["status"] == "answered"
    finally:
        if second.returncode is None:
            second.kill()
            await second.communicate()


async def test_cancelled_write_rolls_back_before_connection_reuse(runtime):
    dsn, _, leases, thread_id = runtime
    lease = await leases.acquire(thread_id)
    async with guarded_saver(dsn, lease) as saver:
        with pytest.raises(asyncio.CancelledError):
            async with saver._cursor(pipeline=True) as cursor:
                await cursor.execute(
                    "UPDATE checkpoint_leases SET fence = fence + 1 WHERE thread_id = %s",
                    (thread_id,),
                )
                raise asyncio.CancelledError
        async with saver._cursor(pipeline=True) as cursor:
            await cursor.execute(
                "SELECT fence FROM checkpoint_leases WHERE thread_id = %s", (thread_id,)
            )
            assert (await cursor.fetchone())["fence"] == lease.fence
