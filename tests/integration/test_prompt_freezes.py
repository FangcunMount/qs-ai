"""Freeze assets with actual MySQL transactions; no model approval is inferred."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.prompt_drafts import CreatePromptDraft, RevisePromptDraft
from qs_ai.application.governance.prompt_freeze import FreezePromptDraft
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.application.interpretation.prompts import InvalidPrompt
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import MySQLPromptFreezer, apply_freeze
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_assets,
    prompt_draft_freezes,
    prompt_draft_revisions,
    prompt_drafts,
)
from tests.integration.test_prompt_drafts import kit as kit

pytestmark = pytest.mark.integration


@pytest.fixture
async def freezing(kit):
    tx, drafts, scope, create, at, _ = kit
    draft = await drafts.apply(scope, create, at)
    command = FreezePromptDraft(draft.draft_id, uuid4(), draft.revision, "冻结语法已校验模板")
    try:
        yield MySQLPromptFreezer(tx), command, draft
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(prompt_draft_freezes).where(
                    prompt_draft_freezes.c.draft_id == str(draft.draft_id)
                )
            )
            await db.execute(
                delete(prompt_assets).where(
                    prompt_assets.c.template_id == draft.template_id,
                    prompt_assets.c.version == draft.target_version,
                )
            )
            await db.commit()


async def test_freeze_replay_receipt_and_edit_lock_preserve_original(kit, freezing):
    tx, drafts, scope, _, at, source = kit
    freezer, command, draft = freezing
    first = await freezer.freeze(scope, command, at)
    assert first == await freezer.freeze(scope, command, at)
    assert first == await MySQLPromptFreezer(tx).get_receipt(scope, command.command_id)
    assert await MySQLPromptAssets(tx).get(source.template_id, source.version) == source
    asset = await MySQLPromptAssets(tx).get(first.asset.identity, first.asset.version)
    assert json.loads(asset.package_json)["Origin"]["snapshot_sha256"] == first.snapshot_sha256
    assert "GitBlobSHA" not in json.loads(asset.package_json)["Ref"]
    with pytest.raises(DraftConflict):
        await drafts.apply(
            scope, RevisePromptDraft(draft.draft_id, uuid4(), 1, draft.content, "继续覆盖"), at
        )
    assert await drafts.get(scope, draft.draft_id) == draft
    with pytest.raises(DraftConflict):
        await freezer.freeze(scope, replace(command, reason="changed"), at)
    with pytest.raises(DraftConflict):
        await freezer.freeze(scope, replace(command, command_id=uuid4()), at)


async def test_same_command_concurrency_freezes_once(kit, freezing):
    tx, _, scope, _, at, _ = kit
    freezer, command, _ = freezing
    results = await asyncio.gather(*(freezer.freeze(scope, command, at) for _ in range(3)))
    assert results[0] == results[1] == results[2]
    async with tx.open() as db:
        rows = (
            await db.execute(
                select(prompt_draft_freezes).where(
                    prompt_draft_freezes.c.draft_id == str(command.draft_id)
                )
            )
        ).all()
        assert len(rows) == 1


async def test_different_commands_race_has_one_winner(kit, freezing):
    _, _, scope, _, at, _ = kit
    freezer, command, _ = freezing
    results = await asyncio.gather(
        freezer.freeze(scope, command, at),
        freezer.freeze(scope, replace(command, command_id=uuid4()), at),
        return_exceptions=True,
    )
    assert sum(isinstance(r, DraftConflict) for r in results) == 1
    assert sum(not isinstance(r, BaseException) for r in results) == 1


async def test_transaction_rollback_leaves_neither_asset_nor_receipt(kit, freezing):
    tx, _, scope, _, at, _ = kit
    freezer, command, draft = freezing
    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
        await apply_freeze(db, scope, command, at)
        await db.rollback()
    with pytest.raises(NotFound):
        await freezer.get_receipt(scope, command.command_id)
    assert await MySQLPromptAssets(tx).get(draft.template_id, draft.target_version) is None
    await freezer.freeze(scope, command, at)


async def test_unfinished_or_stale_revision_has_no_asset(kit, freezing):
    tx, drafts, scope, _, at, _ = kit
    freezer, command, draft = freezing
    await drafts.apply(
        scope,
        RevisePromptDraft(
            draft.draft_id,
            uuid4(),
            1,
            replace(draft.content, task_template="{{unfinished"),
            "编辑中",
        ),
        at,
    )
    with pytest.raises(DraftConflict):
        await freezer.freeze(scope, command, at)
    with pytest.raises(InvalidPrompt):
        await freezer.freeze(scope, replace(command, expected_revision=2), at)
    assert await MySQLPromptAssets(tx).get(draft.template_id, draft.target_version) is None
    with pytest.raises(NotFound):
        await freezer.get_receipt(scope, command.command_id)


async def test_wrong_scope_does_not_reveal_receipts(kit, freezing):
    _, _, scope, _, at, _ = kit
    freezer, command, _ = freezing
    with pytest.raises(NotFound):
        await freezer.freeze(replace(scope, organization_id=2), command, at)
    await freezer.freeze(scope, command, at)
    for other in (replace(scope, organization_id=2), replace(scope, operator_user_id=43)):
        with pytest.raises(NotFound):
            await freezer.get_receipt(other, command.command_id)
        with pytest.raises(NotFound):
            await freezer.freeze(other, command, at)


@pytest.mark.parametrize("field,value", [("receipt_sha256", "0" * 64), ("receipt_json", "{}")])
async def test_corrupt_receipt_is_rejected(kit, freezing, field, value):
    tx, _, scope, _, at, _ = kit
    freezer, command, _ = freezing
    await freezer.freeze(scope, command, at)
    async with tx.open() as db:
        await db.execute(
            update(prompt_draft_freezes)
            .where(prompt_draft_freezes.c.command_id == str(command.command_id))
            .values(**{field: value})
        )
        await db.commit()
    with pytest.raises(ValueError):
        await freezer.get_receipt(scope, command.command_id)


async def test_asset_import_audit_is_bound_to_freeze_receipt(kit, freezing):
    tx, _, scope, _, at, _ = kit
    freezer, command, draft = freezing
    await freezer.freeze(scope, command, at)
    async with tx.open() as db:
        await db.execute(
            update(prompt_assets)
            .where(
                prompt_assets.c.template_id == draft.template_id,
                prompt_assets.c.version == draft.target_version,
            )
            .values(imported_by="other")
        )
        await db.commit()
    with pytest.raises(ValueError):
        await freezer.get_receipt(scope, command.command_id)


async def test_edit_and_freeze_race_never_freezes_unconfirmed_revision(kit, freezing):
    _, drafts, scope, _, at, _ = kit
    freezer, command, draft = freezing
    edit = RevisePromptDraft(
        draft.draft_id,
        uuid4(),
        1,
        replace(draft.content, system_message="新的系统正文"),
        "竞争保存",
    )
    results = await asyncio.gather(
        freezer.freeze(scope, command, at), drafts.apply(scope, edit, at), return_exceptions=True
    )
    assert sum(isinstance(r, DraftConflict) for r in results) == 1
    assert sum(not isinstance(r, BaseException) for r in results) == 1
    current = await drafts.get(scope, draft.draft_id)
    if isinstance(results[0], DraftConflict):
        assert current.revision == 2
        with pytest.raises(NotFound):
            await freezer.get_receipt(scope, command.command_id)
    else:
        assert current == draft


async def test_native_asset_can_seed_a_new_draft_without_overwriting_original(kit, freezing):
    tx, drafts, scope, _, at, _ = kit
    freezer, command, draft = freezing
    original = await freezer.freeze(scope, command, at)
    create = CreatePromptDraft(
        uuid4(),
        uuid4(),
        original.asset,
        draft.template_id,
        draft.target_version + "-next",
        "从冻结资产继续编辑",
    )
    try:
        next_draft = await drafts.apply(scope, create, at)
        assert next_draft.content == draft.content
        revised = await drafts.apply(
            scope,
            RevisePromptDraft(
                create.draft_id,
                uuid4(),
                1,
                replace(next_draft.content, system_message="下一版正文"),
                "新版本",
            ),
            at,
        )
        next_receipt = await freezer.freeze(
            scope, FreezePromptDraft(create.draft_id, uuid4(), 2, "冻结下一版"), at
        )
        assert next_receipt.asset.fingerprint != original.asset.fingerprint
        assert revised.source == original.asset
        assert await freezer.get_receipt(scope, command.command_id) == original
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(prompt_draft_freezes).where(
                    prompt_draft_freezes.c.draft_id == str(create.draft_id)
                )
            )
            await db.execute(
                delete(prompt_draft_revisions).where(
                    prompt_draft_revisions.c.draft_id == str(create.draft_id)
                )
            )
            await db.execute(
                delete(prompt_drafts).where(prompt_drafts.c.draft_id == str(create.draft_id))
            )
            await db.execute(
                delete(prompt_assets).where(
                    prompt_assets.c.template_id == create.template_id,
                    prompt_assets.c.version == create.target_version,
                )
            )
            await db.commit()


async def test_two_drafts_cannot_claim_the_same_target_version(kit, freezing):
    tx, drafts, scope, original, at, _ = kit
    freezer, command, _ = freezing
    create = replace(original, draft_id=uuid4(), command_id=uuid4())
    try:
        await drafts.apply(scope, create, at)
        competing = FreezePromptDraft(create.draft_id, uuid4(), 1, "竞争相同版本")
        results = await asyncio.gather(
            freezer.freeze(scope, command, at),
            freezer.freeze(scope, competing, at),
            return_exceptions=True,
        )
        assert sum(isinstance(r, DraftConflict) for r in results) == 1
        assert sum(not isinstance(r, BaseException) for r in results) == 1
        for cmd, result in zip((command, competing), results, strict=True):
            if isinstance(result, DraftConflict):
                with pytest.raises(NotFound):
                    await freezer.get_receipt(scope, cmd.command_id)
            else:
                assert await freezer.get_receipt(scope, cmd.command_id) == result
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(prompt_draft_freezes).where(
                    prompt_draft_freezes.c.draft_id == str(create.draft_id)
                )
            )
            await db.execute(
                delete(prompt_draft_revisions).where(
                    prompt_draft_revisions.c.draft_id == str(create.draft_id)
                )
            )
            await db.execute(
                delete(prompt_drafts).where(prompt_drafts.c.draft_id == str(create.draft_id))
            )
            await db.commit()
