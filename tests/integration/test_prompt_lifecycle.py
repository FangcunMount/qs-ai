"""Reopened drafts retain freeze state without revealing another actor's receipt."""

import json
from dataclasses import replace
from uuid import UUID, uuid4

import grpc
import pytest
from sqlalchemy import update

from qs_ai.application.governance.prompt_drafts import RevisePromptDraft
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql import prompt_lifecycle as lifecycle_module
from qs_ai.infrastructure.persistence.mysql.prompt_lifecycle import MySQLPromptLifecycleReader
from qs_ai.infrastructure.persistence.mysql.schema import prompt_assets, prompt_draft_freezes
from tests.integration.test_prompt_draft_grpc import rpc_server as rpc_server
from tests.integration.test_prompt_draft_grpc import server as server
from tests.integration.test_prompt_drafts import kit as kit
from tests.integration.test_prompt_freezes import freezing as freezing

pytestmark = pytest.mark.integration


async def test_latest_head_and_freeze_survive_new_reader_for_same_org_admin(kit, freezing):
    tx, drafts, scope, _, at, _ = kit
    freezer, command, draft = freezing
    reader = MySQLPromptLifecycleReader(tx)
    assert (await reader.get(scope, draft.draft_id)).frozen is None
    edited = await drafts.apply(
        scope, RevisePromptDraft(draft.draft_id, uuid4(), 1, draft.content, "调整修订"), at
    )
    assert (await reader.get(scope, draft.draft_id)).draft == edited
    receipt = await freezer.freeze(scope, replace(command, expected_revision=2), at)
    other = replace(scope, operator_user_id=43)
    reopened = await MySQLPromptLifecycleReader(tx).get(other, draft.draft_id)
    assert reopened.draft == edited
    assert reopened.frozen.asset == receipt.asset
    assert reopened.frozen.revision == 2
    assert reopened.frozen.frozen_at == receipt.frozen_at
    with pytest.raises(NotFound):
        await freezer.get_receipt(other, command.command_id)
    with pytest.raises(NotFound):
        await reader.get(replace(scope, organization_id=2), draft.draft_id)
    with pytest.raises(NotFound):
        await reader.get(scope, uuid4())
    with pytest.raises(ValueError):
        await reader.get(scope, UUID(int=0))


async def test_concurrent_edit_and_freeze_cannot_mix_snapshots(kit, freezing, monkeypatch):
    tx, drafts, scope, _, at, _ = kit
    freezer, command, draft = freezing
    original = lifecycle_module.read_draft
    changed = False

    async def read_then_freeze(*args, **kwargs):
        nonlocal changed
        saved = await original(*args, **kwargs)
        if not changed:
            changed = True
            await drafts.apply(
                scope, RevisePromptDraft(draft.draft_id, uuid4(), 1, draft.content, "并发修订"), at
            )
            await freezer.freeze(scope, replace(command, expected_revision=2), at)
        return saved

    monkeypatch.setattr(lifecycle_module, "read_draft", read_then_freeze)
    reader = MySQLPromptLifecycleReader(tx)
    before = await reader.get(scope, draft.draft_id)
    assert before.draft.revision == 1 and before.frozen is None
    after = await reader.get(scope, draft.draft_id)
    assert after.draft.revision == after.frozen.revision == 2
    # The earlier editable view is not authority to modify a now-frozen draft.
    with pytest.raises(DraftConflict):
        await drafts.apply(
            scope, RevisePromptDraft(draft.draft_id, uuid4(), 1, draft.content, "过期视图"), at
        )


@pytest.mark.parametrize("corruption", ["receipt", "organization", "actor", "asset"])
async def test_corrupt_freeze_never_becomes_editable(kit, freezing, corruption):
    tx, _, scope, _, at, _ = kit
    freezer, command, draft = freezing
    await freezer.freeze(scope, command, at)
    async with tx.open() as db:
        if corruption == "asset":
            query = (
                update(prompt_assets)
                .where(
                    prompt_assets.c.template_id == draft.template_id,
                    prompt_assets.c.version == draft.target_version,
                )
                .values(imported_by="corrupt")
            )
        else:
            values = {
                "receipt": {"receipt_sha256": "0" * 64},
                "organization": {"organization_id": 2},
                "actor": {"operator_user_id": 43},
            }[corruption]
            query = (
                update(prompt_draft_freezes)
                .where(prompt_draft_freezes.c.draft_id == str(draft.draft_id))
                .values(**values)
            )
        await db.execute(query)
        await db.commit()
    with pytest.raises(ValueError):
        await MySQLPromptLifecycleReader(tx).get(scope, draft.draft_id)


async def test_rpc_reopen_returns_explicit_lifecycle_without_original_freeze_audit(
    kit, freezing, server
):
    _, _, scope, _, at, _ = kit
    freezer, command, draft = freezing
    query = pb.PromptDraftQuery(
        scope=pb.PublicationScope(organization_id=scope.organization_id, operator_user_id=43),
        draft_id=str(draft.draft_id),
    )
    async with server() as channel:
        client = rpc.PromptDraftManagementStub(channel)
        editable = await client.GetLifecycle(query, timeout=5)
        assert editable.schema_version == "qs-ai-prompt-lifecycle/v1"
        assert editable.status == "editable" and not editable.HasField("frozen")
        assert json.loads(editable.draft.snapshot_json)["revision"] == 1
        receipt = await freezer.freeze(scope, command, at)
        frozen = await client.GetLifecycle(query, timeout=5)
        assert frozen.status == "frozen" and frozen.frozen.revision == 1
        assert frozen.frozen.asset.fingerprint == receipt.asset.fingerprint
        assert str(command.command_id) not in str(frozen)
        assert command.reason not in str(frozen)
        # Existing Get is still the immutable revision, unchanged by freezing.
        assert await client.Get(query, timeout=5) == editable.draft == frozen.draft
        query.revision = 1
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.GetLifecycle(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        query.ClearField("revision")
        query.scope.organization_id = 2
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.GetLifecycle(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
    async with server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.PromptDraftManagementStub(channel).GetLifecycle(query, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
