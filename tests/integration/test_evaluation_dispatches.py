import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import MySQLCheckpoints
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import (
    freeze_policy,
    reserve_dispatch,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_run_policies,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy

pytestmark = pytest.mark.integration


@pytest.fixture
async def prepared():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(database)
    store = MySQLCheckpoints(tx)
    run_id = uuid4()
    at = datetime(2026, 9, 12, tzinfo=UTC)
    cp = ExecutionCheckpoint(
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
    try:
        await store.create(run_id)
        async with tx.open() as db:
            await freeze_policy(db, run_id, load_execution_policy())
            await db.commit()
        await store.save(CheckpointState(run_id, 1, cp), 0)
        yield tx, store, run_id, cp
    finally:
        async with tx.open() as db:
            for table in (evaluation_dispatches, evaluation_run_policies, evaluation_checkpoints):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()
        await database.close()


async def count(tx, run_id):
    async with tx.open() as db:
        return (
            await db.execute(
                select(func.count())
                .select_from(evaluation_dispatches)
                .where(evaluation_dispatches.c.run_id == str(run_id))
            )
        ).scalar_one()


async def test_concurrent_dispatch_reservation_is_once_and_target_budget_is_not_reset(prepared):
    tx, store, run_id, cp = prepared

    async def dispatch(version):
        async with tx.open() as db:
            state = await reserve_dispatch(db, run_id, version, "worker:1", cp.claimed_at)
            await db.commit()
            return state

    results = await asyncio.gather(dispatch(1), dispatch(1), return_exceptions=True)
    assert sum(isinstance(x, CheckpointState) for x in results) == 1
    assert sum(isinstance(x, CheckpointConflict) for x in results) == 1
    assert await count(tx, run_id) == 1
    assert (await store.get(run_id)).version == 2
    second = replace(
        cp, execution_id="execution:2", invocation_id="invocation:2", execution_ordinal=2
    )
    await store.save(CheckpointState(run_id, 3, second), 2)
    await dispatch(3)
    assert await count(tx, run_id) == 2
    third = replace(second, execution_id="execution:3", invocation_id="invocation:3")
    await store.save(CheckpointState(run_id, 5, third), 4)
    with pytest.raises(CheckpointConflict, match="budget"):
        await dispatch(5)
    assert await count(tx, run_id) == 2
    assert (await store.get(run_id)).checkpoint.phase == "prepared"


async def test_dispatch_reservation_rolls_back_ledger_and_checkpoint_together(prepared):
    tx, store, run_id, cp = prepared
    with pytest.raises(RuntimeError, match="injected"):
        async with tx.open() as db:
            await reserve_dispatch(db, run_id, 1, "worker:1", cp.claimed_at)
            raise RuntimeError("injected failure before commit")
    assert await count(tx, run_id) == 0
    assert await store.get(run_id) == CheckpointState(run_id, 1, cp)


async def test_run_budget_counts_prior_dispatches_even_for_other_targets(prepared):
    from sqlalchemy import insert

    from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode

    tx, store, run_id, cp = prepared
    # Synthetic prior ledger entries isolate the run ceiling from NextAction selection.
    async with tx.open() as db:
        await db.execute(
            insert(evaluation_dispatches),
            [
                dict(
                    run_id=str(run_id),
                    invocation_id=f"prior:{i}",
                    execution_id=f"prior-execution:{i}",
                    kind="generation",
                    case_id=f"prior-case:{i}",
                    slot_ordinal=1,
                    candidate_id="",
                    checkpoint_json=encode(cp.mark_dispatching(cp.owner, cp.claimed_at)),
                )
                for i in range(69)
            ],
        )
        await db.commit()
    async with tx.open() as db:
        await reserve_dispatch(db, run_id, 1, cp.owner, cp.claimed_at)
        await db.commit()
    assert await count(tx, run_id) == 70
    fresh_target = replace(cp, case_id="fresh:1", invocation_id="fresh:1", execution_id="fresh:1")
    await store.save(CheckpointState(run_id, 3, fresh_target), 2)
    with pytest.raises(CheckpointConflict, match="budget"):
        async with tx.open() as db:
            await reserve_dispatch(db, run_id, 3, cp.owner, cp.claimed_at)
            await db.commit()
    assert await count(tx, run_id) == 70


@pytest.mark.parametrize("prior_dispatch", [False, True])
async def test_dispatch_rejects_skipped_or_repeated_execution_ordinal(prepared, prior_dispatch):
    tx, store, run_id, cp = prepared
    version = 1
    if prior_dispatch:
        async with tx.open() as db:
            await reserve_dispatch(db, run_id, version, cp.owner, cp.claimed_at)
            await db.commit()
        version += 1
    invalid = replace(
        cp,
        execution_id="execution:next",
        invocation_id="invocation:next",
        execution_ordinal=1 if prior_dispatch else 2,
    )
    await store.save(CheckpointState(run_id, version + 1, invalid), version)
    with pytest.raises(CheckpointConflict, match="ordinal"):
        async with tx.open() as db:
            await reserve_dispatch(db, run_id, version + 1, cp.owner, cp.claimed_at)
            await db.commit()
    assert await count(tx, run_id) == int(prior_dispatch)
    assert await store.get(run_id) == CheckpointState(run_id, version + 1, invalid)


async def test_reused_execution_identity_rolls_back_even_with_new_invocation(prepared):
    tx, store, run_id, cp = prepared
    async with tx.open() as db:
        await reserve_dispatch(db, run_id, 1, cp.owner, cp.claimed_at)
        await db.commit()
    duplicate = replace(cp, invocation_id="invocation:2", execution_ordinal=2)
    await store.save(CheckpointState(run_id, 3, duplicate), 2)
    with pytest.raises(IntegrityError):
        async with tx.open() as db:
            await reserve_dispatch(db, run_id, 3, cp.owner, cp.claimed_at)
            await db.commit()
    assert await count(tx, run_id) == 1
    assert await store.get(run_id) == CheckpointState(run_id, 3, duplicate)
