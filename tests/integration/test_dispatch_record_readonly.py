"""Original dispatch stage budget and immutable ledger; no provider or SDK claim."""

from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, select

from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import (
    plan_dispatch_record,
    reserve_dispatch,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_dispatch_budget_plan_only_selects_then_original_ledger_and_cas_commit_or_rollback(
    ready, semantic, commit
):
    tx, run_id, *_ = ready
    gateway = Gateway(ready)
    version = (await step(ready, gateway)).version if semantic else 3
    at, dispatch_at, expires = (
        AT + timedelta(seconds=1),
        AT + timedelta(seconds=2),
        AT + timedelta(minutes=5),
    )
    execution_id, invocation_id = str(uuid4()), str(uuid4())
    async with tx.open() as db:
        prepared = await prepare_execution(
            db, run_id, version, 1, "worker:original", execution_id, invocation_id, at, expires
        )
        await db.commit()
    before, calls, statements = await snapshot(tx, run_id), gateway.calls, []
    checkpoint = prepared.checkpoint.mark_dispatching("worker:original", dispatch_at)

    def record(_connection, _cursor, statement, *_):
        statements.append(statement.strip().upper())

    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        event.listen(tx.database.engine.sync_engine, "before_cursor_execute", record)
        try:
            plan = await plan_dispatch_record(db, run_id, checkpoint)
        finally:
            event.remove(tx.database.engine.sync_engine, "before_cursor_execute", record)
    assert statements and all(s.startswith("SELECT ") for s in statements)
    assert not any("FOR UPDATE" in s for s in statements)
    assert await snapshot(tx, run_id) == before and gateway.calls == calls
    assert plan.values()["kind"] == ("semantic" if semantic else "generation")
    assert plan.values()["checkpoint_json"]["phase"] == "dispatching"
    async with tx.open() as db:
        state = await reserve_dispatch(db, run_id, prepared.version, "worker:original", dispatch_at)
        assert state.version == version + 2 and state.checkpoint == checkpoint
        if commit:
            await db.commit()
    current = await snapshot(tx, run_id)
    assert current[1][1:] == before[1][1:] and gateway.calls == calls
    if commit:
        assert len(current[1][0]) == len(before[1][0]) + 1
        async with tx.open() as db:
            row = (
                (
                    await db.execute(
                        select(evaluation_dispatches).where(
                            evaluation_dispatches.c.run_id == str(run_id),
                            evaluation_dispatches.c.execution_id == execution_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
        assert all(row[key] == value for key, value in plan.values().items())
        assert current[0][2]["version"] == version + 2
    else:
        assert current == before
