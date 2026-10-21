"""Primary-database planning only; original explicit resolution still owns its root."""

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import (
    accept_resolution,
    plan_resolution,
)
from tests.integration.test_evaluation_recovery import pending, recover
from tests.integration.test_evaluation_resolution import decision
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_unknown_resolution_plan_only_selects_and_matches_original_commit_or_rollback(
    ready, semantic, commit
):
    tx, run_id, *_ = ready
    gateway = Gateway(ready)
    version = (await step(ready, gateway)).version if semantic else 3
    state = await recover(ready, await pending(ready, dispatched=True, version=version))
    before = await snapshot(tx, run_id)
    calls, statements = gateway.calls, []

    def record(_connection, _cursor, statement, *_):
        statements.append(statement.strip().upper())

    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        event.listen(tx.database.engine.sync_engine, "before_cursor_execute", record)
        try:
            plan = await plan_resolution(db, run_id, state.version, 1, decision(), confirm=True)
        finally:
            event.remove(tx.database.engine.sync_engine, "before_cursor_execute", record)
    assert statements and all(s.startswith("SELECT ") for s in statements)
    assert not any("FOR UPDATE" in s for s in statements)
    assert gateway.calls == calls
    assert await snapshot(tx, run_id) == before
    async with tx.open() as db:
        actual = await accept_resolution(db, run_id, state.version, 1, decision(), confirm=True)
        assert actual == plan.state
        if commit:
            await db.commit()
    after = await snapshot(tx, run_id)
    assert after[1] == before[1]  # Original execution evidence is never rewritten.
    if commit:
        run, _, checkpoint = await rows(tx, run_id)
        assert run["progress_json"] == plan.progress()
        assert checkpoint["version"] == plan.state.version
    else:
        assert after == before
