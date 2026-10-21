"""Suite planning only reads; the original root owns case, contract and receipt writes."""

import pytest
from sqlalchemy import event, select

from qs_ai.infrastructure.persistence.mysql.evaluation_suites import (
    RECEIPT,
    apply_registration,
    plan_registration,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_suites
from qs_ai.infrastructure.persistence.mysql.suite_contracts import encode
from tests.integration.test_evaluation_suites import suite_registration as suite_registration
from tests.integration.test_profile_registrations import assets as assets
from tests.integration.test_profile_registrations import complete_release as complete_release
from tests.integration.test_profile_registrations import evaluation_release as evaluation_release
from tests.integration.test_profile_registrations import persisted_assets as persisted_assets
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_profile_registrations import setup_run as setup_run
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("commit", [False, True])
async def test_suite_plan_is_select_only_and_original_root_commits_or_rolls_back(
    suite_registration, commit
):
    tx, _, scope, command, at, *_ = suite_registration
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
            assert (
                await db.scalar(
                    select(evaluation_suites.c.suite_id).where(
                        evaluation_suites.c.suite_id == command.suite_id,
                        evaluation_suites.c.suite_version == command.suite_version,
                    )
                )
                is None
            )
            result = await apply_registration(db, scope, command, at)
            assert result == plan.receipt
            assert db.get_transaction() is original and original.is_active
            if commit:
                await db.commit()
        async with tx.open() as db:
            actual = (
                (
                    await db.execute(
                        select(evaluation_suites).where(
                            evaluation_suites.c.suite_id == command.suite_id,
                            evaluation_suites.c.suite_version == command.suite_version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if commit:
            assert actual["definition_json"] == plan.suite.definition_json
            assert (
                actual["receipt_json"]
                == RECEIPT.dump_json(plan.receipt, exclude_defaults=True).decode()
            )
            assert (actual["contracts_json"], actual["contracts_sha256"]) == encode(plan.contracts)
        else:
            assert actual is None
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
