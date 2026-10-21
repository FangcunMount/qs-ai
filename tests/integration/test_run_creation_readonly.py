"""Read-only creation planning and original root commit/rollback with real MySQL."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql.evaluation_runs import (
    apply_run_creation,
    plan_run_creation,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
@pytest.mark.parametrize("commit", [False, True])
async def test_creation_plan_only_reads_and_apply_stays_on_original_root(setup_run, mode, commit):
    tx, run_id, release = setup_run
    engine = tx.database.engine
    assert engine is not None
    statements = []
    planning = False

    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if planning:
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with tx.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            original = db.get_transaction()
            planning = True
            try:
                plan = await plan_run_creation(
                    db,
                    run_id,
                    release,
                    1,
                    "user:42",
                    "原创建事务验证",
                    datetime(2026, 10, 21, tzinfo=UTC),
                    execution_mode=mode,
                )
            finally:
                planning = False
            assert db.get_transaction() is original and original.is_active
            assert statements and all(s.lstrip().upper().startswith("SELECT") for s in statements)
            assert not any("FOR UPDATE" in s.upper() for s in statements)
            assert await rows(tx, run_id) == [None, None, None]
            state = await apply_run_creation(db, plan)
            assert db.get_transaction() is original and original.is_active
            assert state.run_id == run_id and state.version == 1
            if commit:
                await db.commit()
        actual = await rows(tx, run_id)
        if commit:
            run, policy, checkpoint = actual
            assert run["definition_json"] == plan.definition_json
            assert run["execution_mode"] == mode
            assert policy["definition_json"] == plan.policy.definition_json
            assert checkpoint["version"] == 1 and checkpoint["checkpoint_json"] is None
        else:
            assert actual == [None, None, None]
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
