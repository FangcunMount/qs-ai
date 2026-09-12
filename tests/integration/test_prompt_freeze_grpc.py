"""Native freeze RPC uses real TLS, DI and database, without model evaluation."""

import json
from dataclasses import asdict
from uuid import uuid4

import grpc
import pytest

from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from tests.integration.test_prompt_draft_grpc import rpc_server as rpc_server
from tests.integration.test_prompt_draft_grpc import server as server
from tests.integration.test_prompt_drafts import kit as kit
from tests.integration.test_prompt_freezes import freezing as freezing

pytestmark = pytest.mark.integration


async def test_freeze_then_new_connection_receipt_and_locked_revision(kit, freezing, server):
    _, _, scope, _, _, _ = kit
    _, command, draft = freezing
    request = pb.PromptDraftFreezeCommand(
        scope=pb.PublicationScope(**asdict(scope)),
        draft_id=str(command.draft_id),
        command_id=str(command.command_id),
        expected_revision=1,
        reason=command.reason,
    )
    async with server() as channel:
        client = rpc.PromptDraftManagementStub(channel)
        first = await client.Freeze(request, timeout=5)
        assert first.schema_version == "qs-ai-prompt-freeze/v1"
        body = json.loads(first.receipt_json)
        assert body["asset"]["identity"] == draft.template_id
        assert body["asset"]["version"] == draft.target_version
        assert body["validator_version"] == "qs-ai-prompt-syntax/v1"
        assert await client.Freeze(request, timeout=5) == first
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Revise(
                pb.PromptDraftReviseCommand(
                    scope=request.scope,
                    draft_id=request.draft_id,
                    command_id=str(uuid4()),
                    expected_revision=1,
                    reason="修改冻结版本",
                    content=pb.PromptDraftContent(**asdict(draft.content)),
                ),
                timeout=5,
            )
        assert error.value.code() == grpc.StatusCode.ABORTED
    async with server() as channel:
        result = await rpc.PromptDraftManagementStub(channel).GetFreezeReceipt(
            pb.PromptDraftReceiptQuery(scope=request.scope, command_id=request.command_id),
            timeout=5,
        )
        assert result == first


async def test_freeze_rejects_wrong_identity_scope_and_missing_revision(kit, freezing, server):
    _, _, scope, _, _, _ = kit
    _, command, _ = freezing
    request = pb.PromptDraftFreezeCommand(
        scope=pb.PublicationScope(**asdict(scope)),
        draft_id=str(command.draft_id),
        command_id=str(command.command_id),
        expected_revision=1,
        reason=command.reason,
    )
    async with server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.PromptDraftManagementStub(channel).Freeze(request, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    async with server() as channel:
        client = rpc.PromptDraftManagementStub(channel)
        request.expected_revision = 0
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Freeze(request, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        request.expected_revision = 1
        request.scope.organization_id = 2
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Freeze(request, timeout=5)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        request.scope.organization_id = scope.organization_id
        await client.Freeze(request, timeout=5)
        request.scope.operator_user_id += 1
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.GetFreezeReceipt(
                pb.PromptDraftReceiptQuery(scope=request.scope, command_id=request.command_id),
                timeout=5,
            )
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
