"""Original prepared checkpoint planning is SELECT-only; this is not dispatch or SDK proof."""

from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import (
    plan_prepare_execution,
    prepare_execution,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_frozen_serial_preparation_plan_only_selects_then_original_cas_commits_or_rolls_back(
    ready, semantic, commit
):
    tx, run_id, *_ = ready
    gateway = Gateway(ready)
    version = (await step(ready, gateway)).version if semantic else 3
    before = await snapshot(tx, run_id)
    at, expires = AT + timedelta(seconds=1), AT + timedelta(minutes=5)
    execution_id, invocation_id = str(uuid4()), str(uuid4())
    calls, statements = gateway.calls, []

    def record(_connection, _cursor, statement, *_):
        statements.append(statement.strip().upper())

    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        event.listen(tx.database.engine.sync_engine, "before_cursor_execute", record)
        try:
            plan = await plan_prepare_execution(
                db,
                run_id,
                version,
                1,
                "worker:original",
                execution_id,
                invocation_id,
                at,
                expires,
            )
        finally:
            event.remove(tx.database.engine.sync_engine, "before_cursor_execute", record)
    assert statements and all(s.startswith("SELECT ") for s in statements)
    assert not any("FOR UPDATE" in s for s in statements)
    assert await snapshot(tx, run_id) == before and gateway.calls == calls
    assert plan.checkpoint.phase == "prepared"
    assert plan.checkpoint.kind == ("semantic" if semantic else "generation")
    assert plan.checkpoint.execution_id == execution_id
    assert plan.checkpoint.invocation_id == invocation_id
    assert plan.checkpoint.claimed_at == at and plan.checkpoint.lease_expires_at == expires
    async with tx.open() as db:
        state = await prepare_execution(
            db,
            run_id,
            version,
            1,
            "worker:original",
            execution_id,
            invocation_id,
            at,
            expires,
        )
        assert state == plan
        if commit:
            await db.commit()
    current = await snapshot(tx, run_id)
    assert current[1] == before[1] and gateway.calls == calls
    if commit:
        run, _, checkpoint = await rows(tx, run_id)
        assert run == before[0][0]
        assert checkpoint["version"] == plan.version
        assert decode(checkpoint["checkpoint_json"]) == plan.checkpoint
    else:
        assert current == before
