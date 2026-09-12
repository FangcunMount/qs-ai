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
from qs_ai.transport.grpc.suite_registration import SuiteManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_suites import assets as assets
from tests.integration.test_evaluation_suites import complete_release as complete_release
from tests.integration.test_evaluation_suites import evaluation_release as evaluation_release
from tests.integration.test_evaluation_suites import persisted_assets as persisted_assets
from tests.integration.test_evaluation_suites import registration as registration
from tests.integration.test_evaluation_suites import setup_run as setup_run
from tests.integration.test_evaluation_suites import suite_registration as suite_registration

pytestmark = pytest.mark.integration


@pytest.fixture
async def rpc_server(suite_registration, tmp_path):
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    service = grpc.aio.server()
    rpc.add_SuiteManagementServicer_to_server(SuiteManagement(container), service)
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
        yield port, channel
    finally:
        await service.stop(0)
        await container.close()


def request(context):
    _, _, scope, command, *_ = context
    return pb.SuiteRegisterCommand(
        scope=pb.PublicationScope(**asdict(scope)),
        command_id=str(command.command_id),
        source=pb.FrozenEvaluationRef(**asdict(command.source)),
        suite_id=command.suite_id,
        suite_version=command.suite_version,
        profile=pb.PromptDraftSource(**asdict(command.profile)),
        prompt=pb.PromptDraftSource(**asdict(command.prompt)),
        generation_route=pb.PromptDraftSource(**asdict(command.generation_route)),
        reason=command.reason,
    )


async def test_register_replay_and_new_connection_receipt(suite_registration, rpc_server):
    command = request(suite_registration)
    _, channel_factory = rpc_server
    async with channel_factory() as channel:
        client = rpc.SuiteManagementStub(channel)
        first = await client.Register(command, timeout=5)
        assert first.schema_version == "qs-ai-suite-registration/v1"
        assert json.loads(first.receipt_json)["scope"]["operator_user_id"] == 42
        assert await client.Register(command, timeout=5) == first
    async with channel_factory() as channel:
        client = rpc.SuiteManagementStub(channel)
        query = pb.SuiteRegistrationQuery(scope=command.scope, command_id=command.command_id)
        assert await client.GetReceipt(query, timeout=5) == first
        for org, user in ((2, 42), (1, 43)):
            query.scope.organization_id, query.scope.operator_user_id = org, user
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await client.GetReceipt(query, timeout=5)
            assert error.value.code() == grpc.StatusCode.NOT_FOUND


async def test_wrong_workload_and_changed_source_do_not_register(suite_registration, rpc_server):
    command = request(suite_registration)
    _, channel_factory = rpc_server
    async with channel_factory("other") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.SuiteManagementStub(channel).Register(command, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    command.source.fingerprint = "sha256:" + "0" * 64
    async with channel_factory() as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.SuiteManagementStub(channel).Register(command, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
