import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import MySQLCheckpoints
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints

pytestmark = pytest.mark.integration


async def test_dispatch_and_expired_recovery_have_exactly_one_persisted_winner():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    store = MySQLCheckpoints(transactions)
    run_id = uuid4()
    try:
        assert (await store.create(run_id)).version == 0
        with pytest.raises(CheckpointConflict):
            await store.create(run_id)
        at = datetime(2026, 9, 12, tzinfo=UTC)
        checkpoint = ExecutionCheckpoint(
            "execution:1",
            "generation",
            "case:1",
            1,
            "",
            1,
            "worker:1",
            "invocation:1",
            "prepared",
            at,
            at + timedelta(seconds=30),
        )
        await store.save(CheckpointState(run_id, 1, checkpoint), 0)
        assert (await store.get(run_id)).checkpoint == checkpoint
        assert checkpoint.can_release_preparation(
            "invocation:1", checkpoint.lease_expires_at, checkpoint.lease_expires_at
        )
        dispatched = checkpoint.mark_dispatching("worker:1", checkpoint.lease_expires_at)
        candidates = (CheckpointState(run_id, 2, dispatched), CheckpointState(run_id, 2, None))
        results = await asyncio.gather(
            *(store.save(s, 1) for s in candidates), return_exceptions=True
        )
        assert sum(x is None for x in results) == 1
        assert sum(isinstance(x, CheckpointConflict) for x in results) == 1
        winner = candidates[results.index(None)]
        assert await MySQLCheckpoints(Transactions(database)).get(run_id) == winner
        with pytest.raises(CheckpointConflict):
            await store.save(CheckpointState(run_id, 2, checkpoint), 1)
        with pytest.raises(ValueError):
            await store.save(CheckpointState(run_id, 4, None), 2)
        assert await store.get(run_id) == winner
    finally:
        async with transactions.open() as db:
            await db.execute(
                delete(evaluation_checkpoints).where(evaluation_checkpoints.c.run_id == str(run_id))
            )
            await db.commit()
        await database.close()
