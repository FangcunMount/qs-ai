"""Real failed evidence and original exception; planning is SELECT-only, not authorization."""

import json

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql.evaluation_contract_recovery import (
    authorize_contract_recovery,
    plan_contract_recovery,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_semantic_contract_recovery import blocked, originals

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("commit", [False, True])
async def test_failed_contract_plan_only_selects_and_matches_original_caller_commit_or_rollback(
    ready, commit
):
    tx, run_id, *_ = ready
    state, value = await blocked(ready)
    before = await rows(tx, run_id)
    records = await originals(ready)
    fingerprint = json.loads(before[0]["definition_json"])["release_fingerprint"]
    statements = []

    def record(_db, _cursor, statement, *_):
        statements.append(statement.strip().upper())

    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        event.listen(tx.database.engine.sync_engine, "before_cursor_execute", record)
        try:
            plan = await plan_contract_recovery(
                db, run_id, state.version, 1, fingerprint, value, confirm=True
            )
        finally:
            event.remove(tx.database.engine.sync_engine, "before_cursor_execute", record)
    assert statements and all(s.startswith("SELECT ") for s in statements)
    assert not any("FOR UPDATE" in s for s in statements)
    assert await rows(tx, run_id) == before
    assert await originals(ready) == records
    async with tx.open() as db:
        result = await authorize_contract_recovery(
            db, run_id, state.version, 1, fingerprint, value, confirm=True
        )
        assert result == plan.state
        if commit:
            await db.commit()
    assert await originals(ready) == records
    if commit:
        run, _, checkpoint = await rows(tx, run_id)
        assert run["progress_json"] == plan.progress()
        assert checkpoint["version"] == plan.state.version
    else:
        assert await rows(tx, run_id) == before
