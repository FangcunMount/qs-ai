"""Real mTLS and MySQL; temporary CA and synthetic execution evidence only."""

import os

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_recovery import pending, recover
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("semantic", [False, True])
async def test_mtls_unknown_read_scope_version_evidence_and_original_resolution(
    ready, tmp_path, semantic
):
    tx, run_id, *_ = ready
    version = (await step(ready, Gateway(ready))).version if semantic else 3
    state = await recover(ready, await pending(ready, dispatched=True, version=version))
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://",
                "mysql+asyncmy://",
                1,
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_EvaluationManagementServicer_to_server(EvaluationManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials(
            [(key, cert)],
            root_certificates=ca,
            require_client_auth=True,
        ),
    )
    await server.start()
    channels = []

    def client(certificate):
        channel = grpc.aio.secure_channel(
            f"localhost:{port}",
            grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{certificate}.key").read_bytes(),
                (tmp_path / f"{certificate}.pem").read_bytes(),
            ),
        )
        channels.append(channel)
        return rpc.EvaluationManagementStub(channel)

    request = pb.EvaluationUnknownQuery(
        scope=pb.EvaluationQuery(
            run_id=str(run_id),
            organization_id=1,
            operator_user_id=42,
        ),
        expected_version=state.version,
    )
    try:
        trusted = client("qs")
        before = await snapshot(tx, run_id)
        view = await trusted.ListUnknownExecutions(request, timeout=5)
        assert view.run_id == str(run_id) and view.version == state.version
        assert view.unresolved_result_unknown_count == 1 and view.can_resolve
        target = view.executions[0]
        assert target.execution_id == "execution:dead"
        assert target.kind == ("semantic" if semantic else "generation")
        assert bool(target.candidate_id) is semantic
        assert target.replacement_allowed and target.target_execution_limit == 2
        with pytest.raises(grpc.aio.AioRpcError) as denied:
            await client("other").ListUnknownExecutions(request, timeout=5)
        assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
        for field, value, code in (
            ("organization_id", 2, grpc.StatusCode.NOT_FOUND),
            ("operator_user_id", 0, grpc.StatusCode.INVALID_ARGUMENT),
        ):
            invalid = pb.EvaluationUnknownQuery()
            invalid.CopyFrom(request)
            setattr(invalid.scope, field, value)
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await trusted.ListUnknownExecutions(invalid, timeout=5)
            assert error.value.code() == code
        assert await snapshot(tx, run_id) == before
        # Resolve exactly the inspected execution, preserving provider evidence and using CAS.
        updated = await trusted.ResolveUnknown(
            pb.UnknownResolutionCommand(
                scope=request.scope,
                expected_version=view.version,
                execution_id=target.execution_id,
                decision="cancel_run",
                reason="核对原调用后停止测试任务",
                confirm=True,
                acknowledged_duplicate_call_and_cost_risk=True,
            ),
            timeout=5,
        )
        with pytest.raises(grpc.aio.AioRpcError) as stale:
            await trusted.ListUnknownExecutions(request, timeout=5)
        assert stale.value.code() == grpc.StatusCode.ABORTED
        request.expected_version = updated.version
        current = await trusted.ListUnknownExecutions(request, timeout=5)
        assert current.status == "canceled" and not current.can_resolve and not current.executions
        assert (await snapshot(tx, run_id))[1] == before[1]
    finally:
        for channel in channels:
            await channel.close()
        await server.stop(0)
        await container.close()
