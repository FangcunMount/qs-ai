"""Mixed request scopes contend for one real pool; no model or business writes."""

import asyncio
import os
import time

import pytest
from sqlalchemy import event, text

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions

pytestmark = pytest.mark.integration


async def test_five_responsibilities_share_pool_without_sharing_transactions():
    url = os.environ.get("QS_AI_DATABASE_URL")
    if not url:
        pytest.skip("Requires disposable MySQL")
    container = create_container(Settings(database_url=url))
    database = await container.get(Database)
    assert database.engine is not None
    active = peak = 0
    sessions = []
    timings = []

    @event.listens_for(database.engine.sync_engine, "checkout")
    def checkout(*args):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)

    @event.listens_for(database.engine.sync_engine, "checkin")
    def checkin(*args):
        nonlocal active
        active -= 1

    async def responsibility(name, concurrency):
        gate = asyncio.Semaphore(concurrency)

        async def operation():
            async with gate, container() as scope:
                assert await scope.get(Database) is database
                tx = await scope.get(Transactions)
                started = time.monotonic()
                async with tx.open() as session:
                    sessions.append(session)
                    await session.execute(text("SELECT SLEEP(0.02)"))
                    await session.commit()
                timings.append(time.monotonic() - started)

        await asyncio.gather(*(operation() for _ in range(20)))

    try:
        await asyncio.wait_for(
            asyncio.gather(
                responsibility("http", 8),
                responsibility("grpc", 8),
                responsibility("generation", 1),
                responsibility("evaluation", 1),
                responsibility("delivery", 1),
            ),
            15,
        )
        assert len(sessions) == len({id(s) for s in sessions}) == 100
        assert active == 0 and 5 <= peak <= 10
        assert max(timings) < 3  # Default pool wait bound, including SQL work.
        # Losing a connection must not poison the shared pool for subsequent scopes.
        async with database.engine.connect() as connection:
            await connection.invalidate()
        async with container() as scope:
            tx = await scope.get(Transactions)
            async with tx.open() as session:
                assert await session.scalar(text("SELECT 1")) == 1
    finally:
        await container.close()
