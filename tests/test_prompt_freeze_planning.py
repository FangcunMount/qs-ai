"""Characterize original freeze writes before extracting a read-only asset plan."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.prompt_freeze import FreezePromptDraft
from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.infrastructure.persistence.mysql import prompt_freezes as freezes
from tests.test_native_prompts import draft as draft


@pytest.fixture
def original(draft, monkeypatch):
    scope = DraftScope(1, 42)
    command = FreezePromptDraft(draft.draft_id, uuid4(), draft.revision, "显式冻结计划")
    source = next(v for v in baseline_assets()[1] if v.fingerprint == draft.source.fingerprint)
    rows = Mock()
    rows.first.return_value = None
    db = SimpleNamespace(
        execute=AsyncMock(return_value=rows), commit=AsyncMock(), rollback=AsyncMock()
    )
    prior, read = AsyncMock(return_value=None), AsyncMock(return_value=draft)
    reader = SimpleNamespace(get=AsyncMock(side_effect=[source, None]))
    monkeypatch.setattr(freezes, "load_receipt", prior)
    monkeypatch.setattr(freezes, "read_draft", read)
    monkeypatch.setattr(freezes, "AssetSnapshotReader", Mock(return_value=reader))
    return SimpleNamespace(**locals())


async def test_original_freeze_preserves_sql_order_exact_asset_receipt_and_caller_root(original):
    f = original
    receipt = await freezes.apply_freeze(f.db, f.scope, f.command, f.draft.saved_at)
    assert f.prior.await_count == 2
    f.read.assert_awaited_once_with(f.db, f.scope, f.command.draft_id, lock=True)
    statements = [c.args[0] for c in f.db.execute.await_args_list]
    assert statements[0].is_select
    assert [s.table.name for s in statements[1:]] == [
        "governance_asset_versions",
        "governance_prompt_draft_freezes",
    ]
    asset = freezes.freeze_asset(f.draft, freezes.checksum(freezes.SNAPSHOT.dump_json(f.draft)))
    assert statements[1].compile().params["body_bytes"] == asset.package_json.encode()
    values = statements[2].compile().params
    assert values["receipt_json"] == freezes.RECEIPT.dump_json(receipt).decode()
    assert values["receipt_sha256"] == freezes.checksum(freezes.RECEIPT.dump_json(receipt))
    f.db.commit.assert_not_called()
    f.db.rollback.assert_not_called()


async def test_original_replay_never_locks_draft_or_writes_again(original):
    f = original
    receipt = await freezes.apply_freeze(f.db, f.scope, f.command, f.draft.saved_at)
    f.prior.return_value = receipt
    f.db.execute.reset_mock()
    f.read.reset_mock()
    assert await freezes.apply_freeze(f.db, f.scope, f.command, f.draft.saved_at) == receipt
    f.db.execute.assert_not_called()
    f.read.assert_not_called()


async def test_read_only_freeze_plan_checks_original_rules_without_lock_or_writes(original):
    f = original
    plan = await freezes.plan_freeze(f.db, f.scope, f.command, f.draft.saved_at, lock=False)
    assert plan.asset is not None
    assert plan.receipt.scope == f.scope and plan.receipt.command == f.command
    assert plan.receipt.frozen_at == f.draft.saved_at
    assert plan.asset == freezes.freeze_asset(
        f.draft, freezes.checksum(freezes.SNAPSHOT.dump_json(f.draft))
    )
    assert f.db.execute.await_count == 1 and f.db.execute.await_args.args[0].is_select
    f.read.assert_awaited_once_with(f.db, f.scope, f.command.draft_id, lock=False)
    f.db.commit.assert_not_called()
    f.db.rollback.assert_not_called()
    from dataclasses import FrozenInstanceError

    with pytest.raises(FrozenInstanceError):
        plan.asset = None
    assert f.draft.content.system_message not in repr(plan)


@pytest.mark.parametrize("invalid", ["revision", "source", "target", "time"])
async def test_readonly_freeze_plan_refuses_original_conflicts_without_business_writes(
    original, invalid
):
    from dataclasses import replace

    f = original
    command, at = f.command, f.draft.saved_at
    if invalid == "revision":
        command = replace(command, expected_revision=2)
    elif invalid == "source":
        f.reader.get.side_effect = [None]
    elif invalid == "target":
        source = next(
            v for v in baseline_assets()[1] if v.fingerprint == f.draft.source.fingerprint
        )
        f.reader.get.side_effect = [source, source]
    else:
        at = at.replace(tzinfo=None)
    with pytest.raises(ValueError):
        await freezes.plan_freeze(f.db, f.scope, command, at, lock=False)
    assert all(c.args[0].is_select for c in f.db.execute.await_args_list)
    f.db.commit.assert_not_called()
    f.db.rollback.assert_not_called()
