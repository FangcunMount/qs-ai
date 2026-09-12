"""Actual mTLS, RPC, Dishka and MySQL draft lifecycle; no model or IAM impersonation."""

import json
import os
from dataclasses import asdict
from uuid import uuid4

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
from qs_ai.transport.grpc.prompt_drafts import PromptDraftManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_prompt_drafts import kit as kit

pytestmark = pytest.mark.integration


@pytest.fixture
async def server(kit, tmp_path):
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_PromptDraftManagementServicer_to_server(PromptDraftManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    def channel(identity="qs"):
        return grpc.aio.secure_channel(
            f"localhost:{port}",
            grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            ),
        )

    try:
        yield channel
    finally:
        await server.stop(0)
        await container.close()


def request(kit):
    _, _, scope, command, _, _ = kit
    return pb.PromptDraftCreateCommand(
        scope=pb.PublicationScope(**asdict(scope)),
        draft_id=str(command.draft_id),
        command_id=str(command.command_id),
        source=pb.PromptDraftSource(**asdict(command.source)),
        template_id=command.template_id,
        target_version=command.target_version,
        reason=command.reason,
    )


async def test_rpc_create_edit_history_and_new_connection_receipt(kit, server):
    create = request(kit)
    async with server() as channel:
        client = rpc.PromptDraftManagementStub(channel)
        first = await client.Create(create, timeout=5)
        assert first.schema_version == "qs-ai-prompt-draft/v1" and first.revision == 1
        body = json.loads(first.snapshot_json)
        body["content"]["task_template"] = "尚在编辑 {{unfinished}}"
        edit = pb.PromptDraftReviseCommand(
            scope=create.scope,
            draft_id=create.draft_id,
            command_id=str(uuid4()),
            expected_revision=1,
            content=pb.PromptDraftContent(**body["content"]),
            reason="修订草稿",
        )
        second = await client.Revise(edit, timeout=5)
        assert second.revision == 2
        assert await client.Create(create, timeout=5) == first
        assert await client.Revise(edit, timeout=5) == second
        assert (
            await client.Get(
                pb.PromptDraftQuery(scope=create.scope, draft_id=create.draft_id), timeout=5
            )
            == second
        )
        assert (
            await client.Get(
                pb.PromptDraftQuery(scope=create.scope, draft_id=create.draft_id, revision=1),
                timeout=5,
            )
            == first
        )
    # A lost mutation response is recovered using its original command, not a new edit.
    async with server() as channel:
        recovered = await rpc.PromptDraftManagementStub(channel).GetReceipt(
            pb.PromptDraftReceiptQuery(scope=create.scope, command_id=edit.command_id), timeout=5
        )
        assert recovered == second


async def test_wrong_workload_and_scope_are_rejected(kit, server):
    create = request(kit)
    async with server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.PromptDraftManagementStub(channel).Create(create, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    async with server() as channel:
        client = rpc.PromptDraftManagementStub(channel)
        await client.Create(create, timeout=5)
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Get(
                pb.PromptDraftQuery(
                    scope=pb.PublicationScope(organization_id=2, operator_user_id=42),
                    draft_id=create.draft_id,
                ),
                timeout=5,
            )
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.GetReceipt(
                pb.PromptDraftReceiptQuery(
                    scope=pb.PublicationScope(organization_id=1, operator_user_id=43),
                    command_id=create.command_id,
                ),
                timeout=5,
            )
        assert error.value.code() == grpc.StatusCode.NOT_FOUND


async def test_malformed_revision_stale_write_and_dependency_errors_are_bounded(
    kit, server, monkeypatch
):
    create = request(kit)
    async with server() as channel:
        client = rpc.PromptDraftManagementStub(channel)
        first = await client.Create(create, timeout=5)
        body = json.loads(first.snapshot_json)
        missing = pb.PromptDraftReviseCommand(
            scope=create.scope,
            draft_id=create.draft_id,
            command_id=str(uuid4()),
            expected_revision=1,
            reason="缺少正文",
        )
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Revise(missing, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        stale = pb.PromptDraftReviseCommand(
            scope=create.scope,
            draft_id=create.draft_id,
            command_id=str(uuid4()),
            expected_revision=2,
            content=pb.PromptDraftContent(**body["content"]),
            reason="错误版本",
        )
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Revise(stale, timeout=5)
        assert error.value.code() == grpc.StatusCode.ABORTED
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Get(
                pb.PromptDraftQuery(scope=create.scope, draft_id=create.draft_id, revision=0),
                timeout=5,
            )
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        async def unavailable(*args):
            raise RuntimeError("private-database-diagnostic")

        monkeypatch.setattr(MySQLPromptDrafts, "get", unavailable)
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Get(
                pb.PromptDraftQuery(scope=create.scope, draft_id=create.draft_id), timeout=5
            )
        assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        assert "private-database" not in error.value.details()
