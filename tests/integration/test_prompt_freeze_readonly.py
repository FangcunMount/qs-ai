"""No writes while planning a native Prompt; original root owns all persistence."""

import pytest
from sqlalchemy import event, select

from qs_ai.infrastructure.persistence.mysql.asset_records import prompt_assets
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import RECEIPT, apply_freeze, plan_freeze
from qs_ai.infrastructure.persistence.mysql.schema import prompt_draft_freezes
from tests.integration.test_prompt_drafts import kit as kit
from tests.integration.test_prompt_freezes import freezing as freezing
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("commit", [False, True])
async def test_prompt_freeze_plan_is_select_only_and_original_apply_commits_or_rolls_back(
    kit, freezing, commit
):
    tx, _, scope, _, at, _ = kit
    _, command, draft = freezing
    engine = tx.database.engine
    statements, planning = [], False

    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if planning:
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with tx.open() as db:
            await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
            original = db.get_transaction()
            planning = True
            try:
                plan = await plan_freeze(db, scope, command, at)
            finally:
                planning = False
            assert plan.asset is not None
            assert statements and all(v.lstrip().upper().startswith("SELECT") for v in statements)
            assert not any("FOR UPDATE" in v.upper() for v in statements)
            assert db.get_transaction() is original and original.is_active
            assert (
                await db.scalar(
                    select(prompt_draft_freezes.c.command_id).where(
                        prompt_draft_freezes.c.command_id == str(command.command_id)
                    )
                )
                is None
            )
            result = await apply_freeze(db, scope, command, at)
            assert result == plan.receipt
            assert db.get_transaction() is original and original.is_active
            if commit:
                await db.commit()
        async with tx.open() as db:
            actual = (
                (
                    await db.execute(
                        select(prompt_assets).where(
                            prompt_assets.c.template_id == draft.template_id,
                            prompt_assets.c.version == draft.target_version,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            receipt = (
                (
                    await db.execute(
                        select(prompt_draft_freezes).where(
                            prompt_draft_freezes.c.command_id == str(command.command_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if commit:
                assert actual["package_json"] == plan.asset.package_json
                assert receipt["receipt_json"] == RECEIPT.dump_json(plan.receipt).decode()
            else:
                assert actual is None and receipt is None
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
