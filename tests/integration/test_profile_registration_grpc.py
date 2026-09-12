"""Real mTLS, request DI and MySQL registration; QS actor authorization is upstream."""

import json
import os
from dataclasses import asdict

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.profile_registration import ProfileManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_profile_registrations import assets as assets
from tests.integration.test_profile_registrations import complete_release as complete_release
from tests.integration.test_profile_registrations import evaluation_release as evaluation_release
from tests.integration.test_profile_registrations import persisted_assets as persisted_assets
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_profile_registrations import setup_run as setup_run

pytestmark = pytest.mark.integration


@pytest.fixture
async def server(registration, tmp_path):
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    service = grpc.aio.server()
    rpc.add_ProfileManagementServicer_to_server(ProfileManagement(container), service)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = service.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await service.start()

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
        await service.stop(0)
        await container.close()


def request(registration):
    _, _, scope, command, _, _ = registration
    return pb.ProfileRegisterCommand(
        scope=pb.PublicationScope(**asdict(scope)),
        command_id=str(command.command_id),
        source=pb.PromptDraftSource(**asdict(command.source)),
        definition_json=command.definition_json,
        prompt=pb.PromptDraftSource(**asdict(command.prompt)),
        generation_route=pb.PromptDraftSource(**asdict(command.generation_route)),
        reason=command.reason,
    )


async def test_register_replay_and_new_connection_scoped_receipt(registration, server):
    command = request(registration)
    async with server() as channel:
        client = rpc.ProfileManagementStub(channel)
        first = await client.Register(command, timeout=5)
        assert first.schema_version == "qs-ai-profile-registration/v1"
        assert json.loads(first.receipt_json)["scope"]["operator_user_id"] == 42
        assert await client.Register(command, timeout=5) == first
    async with server() as channel:
        client = rpc.ProfileManagementStub(channel)
        query = pb.ProfileRegistrationQuery(scope=command.scope, command_id=command.command_id)
        assert await client.GetReceipt(query, timeout=5) == first
        for org, user in ((2, 42), (1, 43)):
            query.scope.organization_id, query.scope.operator_user_id = org, user
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await client.GetReceipt(query, timeout=5)
            assert error.value.code() == grpc.StatusCode.NOT_FOUND


async def test_wrong_workload_and_invalid_policy_do_not_register(registration, server):
    from tests.integration.test_profile_registrations import records

    command = request(registration)
    async with server("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.ProfileManagementStub(channel).Register(command, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    command.definition_json = '{"api_key":"private-example"}'
    async with server() as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.ProfileManagementStub(channel).Register(command, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "private-example" not in error.value.details()
    assert [len(r) for r in await records(registration[0], registration[3])] == [0, 0]
