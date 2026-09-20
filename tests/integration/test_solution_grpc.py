"""Production DI + real mTLS and MySQL; no provider calls or human decisions."""

import json
import os
from dataclasses import asdict

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.solutions import SolutionManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_solutions import (
    assets as assets,
)
from tests.integration.test_solutions import (
    complete_release as complete_release,
)
from tests.integration.test_solutions import (
    evaluation_release as evaluation_release,
)
from tests.integration.test_solutions import (
    persisted_assets as persisted_assets,
)
from tests.integration.test_solutions import (
    setup_run as setup_run,
)
from tests.integration.test_solutions import (
    workspace as workspace,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def solution_rpc(workspace, tmp_path):
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_SolutionManagementServicer_to_server(SolutionManagement(container), server)
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
        yield port, channel
    finally:
        await server.stop(0)
        await container.close()


async def test_authenticated_creation_and_receipt_recovery(workspace, solution_rpc):
    _, _, scope, sid, create, _ = workspace
    _, channel = solution_rpc
    command = pb.SolutionWrite(
        scope=pb.PublicationScope(**asdict(scope)),
        solution_id=str(sid),
        command_json=json.dumps(asdict(create), default=str),
    )
    async with channel() as connection:
        client = rpc.SolutionManagementStub(connection)
        result = await client.Create(command, timeout=10)
        assert result.schema_version == "qs-ai-solution/v1"
        assert json.loads(result.data_json)["revision"] == 1
        assert await client.Create(command, timeout=10) == result
    async with channel() as connection:
        client = rpc.SolutionManagementStub(connection)
        query = pb.SolutionQuery(scope=command.scope, command_id=str(create.command_id))
        assert await client.GetReceipt(query, timeout=10) == result
        query.scope.operator_user_id += 1
        with pytest.raises(grpc.aio.AioRpcError) as failure:
            await client.GetReceipt(query, timeout=10)
        assert failure.value.code() == grpc.StatusCode.NOT_FOUND
        capabilities = json.loads(
            (await client.GetModels(pb.SolutionQuery(scope=command.scope), timeout=10)).data_json
        )
        assert capabilities["models"] == ["deepseek-v4-pro"]
        assert "api_key" not in capabilities and "endpoint" not in capabilities


async def test_untrusted_workload_and_invalid_wire_never_write(workspace, solution_rpc):
    _, store, scope, sid, create, _ = workspace
    _, channel = solution_rpc
    command = pb.SolutionWrite(
        scope=pb.PublicationScope(**asdict(scope)),
        solution_id=str(sid),
        command_json=json.dumps(asdict(create), default=str),
    )
    async with channel("other") as connection:
        with pytest.raises(grpc.aio.AioRpcError) as failure:
            await rpc.SolutionManagementStub(connection).Create(command, timeout=10)
        assert failure.value.code() == grpc.StatusCode.PERMISSION_DENIED
    async with channel() as connection:
        for raw in (
            "null",
            '{"secret":"not-echoed"}',
            '{"command_id":1}',
            '{"command_id":"a","command_id":"b"}',
        ):
            command.command_json = raw
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await rpc.SolutionManagementStub(connection).Create(command, timeout=10)
            assert failure.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            assert "not-echoed" not in failure.value.details()
    assert not any(row["solution_id"] == str(sid) for row in (await store.list(scope))["items"])


@pytest.mark.interop
async def test_go_authorized_proxy_to_python_persistent_receipt(
    workspace, solution_rpc, go_management, tmp_path
):
    import asyncio

    _, _, scope, sid, command, _ = workspace
    port, _ = solution_rpc
    body = dict(
        Action="solution",
        SolutionWrite=True,
        SolutionOperation="create",
        SolutionID=str(sid),
        SolutionBody=asdict(command),
        OrgID=scope.organization_id,
        UserID=scope.operator_user_id,
        Allowed=True,
    )

    async def call(**changes):
        process = await asyncio.create_subprocess_exec(
            str(go_management),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / "qs.pem"),
            str(tmp_path / "qs.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(json.dumps({**body, **changes}, default=str).encode()), 20
            )
            assert process.returncode == 0
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    assert (await call(Allowed=False))["Denied"]
    assert (await call(AuditOnly=True))["Denied"]
    accepted = await call()
    assert accepted["Code"] == "OK" and accepted["State"]["solution_id"] == str(sid)
    receipt = await call(
        SolutionWrite=False, SolutionOperation="receipt", SolutionID=str(command.command_id)
    )
    assert receipt == accepted
    assert (await call(SolutionWrite=False, SolutionOperation="get", OrgID=2))["Code"] == "NotFound"
