"""Prompt edit commands use real transactions; no publication or model execution."""

import asyncio
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.prompt_drafts import (
    CreatePromptDraft,
    DraftScope,
    RevisePromptDraft,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts, apply_draft
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_assets,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_draft_revisions as revisions,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_drafts as heads,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def kit():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    db = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(db)
    source, values = baseline_assets()
    asset = next(a for a in values if a.version == "v6")
    inserted = await MySQLPromptAssets(tx).put(asset, source, "test:prompt-drafts")
    scope = DraftScope(1, 42)
    command = CreatePromptDraft(
        uuid4(),
        uuid4(),
        AssetReference(asset.template_id, asset.version, asset.fingerprint, asset.package_sha256),
        asset.template_id,
        "draft-" + str(uuid4()),
        "修改已有 Prompt",
    )
    try:
        yield tx, MySQLPromptDrafts(tx), scope, command, datetime.now(UTC), asset
    finally:
        async with tx.open() as connection:
            await connection.execute(
                delete(revisions).where(revisions.c.draft_id == str(command.draft_id))
            )
            await connection.execute(delete(heads).where(heads.c.draft_id == str(command.draft_id)))
            if inserted:
                await connection.execute(
                    delete(prompt_assets).where(
                        prompt_assets.c.template_id == asset.template_id,
                        prompt_assets.c.version == asset.version,
                    )
                )
            await connection.commit()
        await db.close()


async def test_create_edit_replay_and_history_leave_source_asset_unchanged(kit):
    tx, store, scope, command, at, asset = kit
    first = await store.apply(scope, command, at)
    assert first.revision == 1
    assert first.content.system_message == json.loads(asset.package_json)["SystemMessage"]
    edit = RevisePromptDraft(
        first.draft_id,
        uuid4(),
        1,
        replace(first.content, system_message="新的系统指令"),
        "改进表达",
    )
    second = await store.apply(scope, edit, at + timedelta(seconds=1))
    assert second.revision == 2 and second.source == first.source
    assert await store.get(scope, first.draft_id) == second
    assert await store.get(scope, first.draft_id, 1) == first
    assert await store.get_receipt(scope, edit.command_id) == second
    assert await store.apply(scope, command, at + timedelta(seconds=2)) == first
    assert await store.apply(scope, edit, at + timedelta(seconds=3)) == second
    assert await MySQLPromptAssets(tx).get(asset.template_id, asset.version) == asset
    assert await MySQLPromptAssets(tx).get(command.template_id, command.target_version) is None


async def test_identical_concurrent_commands_have_one_revision_and_same_receipt(kit):
    tx, store, scope, command, at, _ = kit
    first, duplicate = await asyncio.gather(
        store.apply(scope, command, at), store.apply(scope, command, at)
    )
    assert first == duplicate
    edit = RevisePromptDraft(
        first.draft_id, uuid4(), 1, replace(first.content, task_template="编辑中"), "保存草稿"
    )
    second, duplicate = await asyncio.gather(
        store.apply(scope, edit, at), store.apply(scope, edit, at)
    )
    assert second == duplicate and second.revision == 2
    async with tx.open() as db:
        assert (
            len(
                (
                    await db.execute(
                        select(revisions).where(revisions.c.draft_id == str(first.draft_id))
                    )
                ).all()
            )
            == 2
        )


async def test_different_concurrent_edits_only_accept_one_without_lost_update(kit):
    _, store, scope, command, at, _ = kit
    first = await store.apply(scope, command, at)
    edits = [
        RevisePromptDraft(
            first.draft_id, uuid4(), 1, replace(first.content, task_template=value), "编辑"
        )
        for value in ("甲", "乙")
    ]
    outcomes = await asyncio.gather(
        *(store.apply(scope, c, at) for c in edits), return_exceptions=True
    )
    assert sum(isinstance(value, DraftConflict) for value in outcomes) == 1
    accepted = next(value for value in outcomes if not isinstance(value, Exception))
    assert accepted == await store.get(scope, first.draft_id)
    assert await store.get(scope, first.draft_id, 1) == first


async def test_rollbacks_leave_neither_partial_create_nor_partial_edit(kit):
    tx, store, scope, command, at, _ = kit
    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
        await apply_draft(db, scope, command, at)
    with pytest.raises(NotFound):
        await store.get(scope, command.draft_id)
    with pytest.raises(NotFound):
        await store.get_receipt(scope, command.command_id)
    first = await store.apply(scope, command, at)
    edit = RevisePromptDraft(first.draft_id, uuid4(), 1, first.content, "再保存")
    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
        await apply_draft(db, scope, edit, at)
    assert await store.get(scope, first.draft_id) == first
    with pytest.raises(NotFound):
        await store.get_receipt(scope, edit.command_id)


async def test_foreign_org_and_other_operators_cannot_read_command_receipts(kit):
    _, store, scope, command, at, _ = kit
    first = await store.apply(scope, command, at)
    foreign = replace(scope, organization_id=2)
    with pytest.raises(NotFound):
        await store.get(foreign, first.draft_id)
    edit = RevisePromptDraft(first.draft_id, uuid4(), 1, first.content, "外部修改")
    with pytest.raises(NotFound):
        await store.apply(foreign, edit, at)
    for denied in (foreign, replace(scope, operator_user_id=43)):
        with pytest.raises(NotFound):
            await store.get_receipt(denied, command.command_id)
    # Other authorized administrators in the same org can read revision history.
    assert await store.get(replace(scope, operator_user_id=43), first.draft_id) == first


async def test_source_drift_existing_target_and_changed_command_are_rejected(kit):
    _, store, scope, command, at, _ = kit
    with pytest.raises(ValueError, match="source"):
        await store.apply(
            scope, replace(command, source=replace(command.source, content_sha256="a" * 64)), at
        )
    with pytest.raises(DraftConflict, match="immutable"):
        await store.apply(scope, replace(command, target_version=command.source.version), at)
    first = await store.apply(scope, command, at)
    with pytest.raises(DraftConflict, match="different"):
        await store.apply(scope, replace(command, reason="另一项请求"), at)
    assert await store.get(scope, first.draft_id) == first


@pytest.mark.parametrize("damage", ["snapshot", "audit", "index", "history"])
async def test_corrupt_revision_never_returns_plausible_draft(kit, damage):
    tx, store, scope, command, at, _ = kit
    await store.apply(scope, command, at)
    async with tx.open() as db:
        where = revisions.c.command_id == str(command.command_id)
        if damage == "snapshot":
            await db.execute(update(revisions).where(where).values(snapshot_sha256="a" * 64))
        elif damage == "audit":
            raw = json.loads(await db.scalar(select(revisions.c.request_json).where(where)))
            raw["command"]["reason"] = "篡改理由"
            await db.execute(update(revisions).where(where).values(request_json=json.dumps(raw)))
        elif damage == "index":
            await db.execute(update(revisions).where(where).values(operator_user_id=43))
        else:
            await db.execute(delete(revisions).where(where))
        await db.commit()
    with pytest.raises(ValueError):
        await store.get(scope, command.draft_id)
