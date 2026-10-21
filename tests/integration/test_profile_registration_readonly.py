"""Read-only future Profile validation; original registration still owns the same root."""

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql.profile_registrations import (
    apply_registration,
    plan_registration,
)
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_profile_registrations import records
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("commit", [False, True])
async def test_profile_plan_only_reads_then_original_registration_owns_commit_or_rollback(
    registration, commit
):
    tx, _, scope, command, at, _ = registration
    engine = tx.database.engine
    statements, planning = [], False

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
                plan = await plan_registration(db, scope, command, at)
            finally:
                planning = False
            assert not plan.existing
            assert statements and all(v.lstrip().upper().startswith("SELECT") for v in statements)
            assert not any("FOR UPDATE" in v.upper() for v in statements)
            assert db.get_transaction() is original and original.is_active
            assert await records(tx, command) == [[], []]
            result = await apply_registration(db, scope, command, at)
            assert result == plan.receipt
            assert db.get_transaction() is original and original.is_active
            if commit:
                await db.commit()
        actual = await records(tx, command)
        if commit:
            assert [len(v) for v in actual] == [1, 1]
            assert actual[0][0]["definition_json"] == plan.profile.definition_json
        else:
            assert actual == [[], []]
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
