"""QS authorization/client -> mTLS -> AI diagnostic snapshot, using synthetic IAM/calls."""

import asyncio
import base64
import hashlib
import json
import os

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_diagnostics import InvalidOutput
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.mark.parametrize("kind", ["generation", "semantic", "failed", "unknown"])
async def test_qs_reads_execution_evidence_and_refuses_wrong_scope(
    ready, go_management, tmp_path, kind
):
    tx, run_id, *_ = ready
    gateway = InvalidOutput(ready) if kind == "failed" else Gateway(ready, fail=kind == "unknown")
    state = await step(ready, gateway)
    if kind == "semantic":
        state = await step(ready, gateway, version=state.version)
    before = await snapshot(tx, run_id)
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_EvaluationManagementServicer_to_server(EvaluationManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    async def call(certificate="qs", **changes):
        request = dict(
            Action="executions",
            OrgID=1,
            UserID=42,
            Allowed=True,
            AuditOnly=True,
            RunID=str(run_id),
            Version=state.version,
            Executions={"Limit": 1},
        )
        request.update(changes)
        process = await asyncio.create_subprocess_exec(
            str(go_management),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / f"{certificate}.pem"),
            str(tmp_path / f"{certificate}.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(json.dumps(request).encode()), 15
            )
            assert process.returncode == 0
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    try:
        page = await call()
        assert page["Code"] == "OK" and len(page["State"]["executions"]) == 1
        item = page["State"]["executions"][0]
        if kind == "semantic":
            next_page = await call(Executions={"Limit": 1, "Cursor": page["State"]["next_cursor"]})
            assert next_page["Code"] == "OK" and not next_page["State"]["next_cursor"]
            item = next(
                x for x in [item, *next_page["State"]["executions"]] if x["kind"] == "semantic"
            )
        result = await call(Action="execution-output", ExecutionID=item["execution_id"])
        assert result["Code"] == "OK"
        value = result["State"]
        assert value["execution"] == item
        body = base64.b64decode(value["raw_output"])
        assert hashlib.sha256(body).hexdigest() == value["raw_sha256"]
        assert (
            hashlib.sha256(base64.b64decode(value["normalized_output"])).hexdigest()
            == value["normalized_sha256"]
        )
        if kind == "failed":
            assert b"not-json" in body and item["status"] == "failed"
        if kind == "unknown":
            assert item["status"] == "result_unknown"
        assert (await call(Allowed=False))["Denied"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert (await call(OrgID=2))["Code"] == "NotFound"
        assert (await call(Version=state.version - 1))["Code"] == "Aborted"
        assert (await call(Action="execution-output", ExecutionID=item["execution_id"], OrgID=2))[
            "Code"
        ] == "NotFound"
        assert (
            await call(Action="execution-output", ExecutionID=item["execution_id"], Allowed=False)
        )["Denied"]
        assert (await call(Action="start", Confirm=True))["Denied"]
        credentials = grpc.ssl_channel_credentials(
            ca, (tmp_path / "qs.key").read_bytes(), (tmp_path / "qs.pem").read_bytes()
        )
        async with grpc.aio.secure_channel(f"localhost:{port}", credentials) as channel:
            client = rpc.EvaluationManagementStub(channel)
            with pytest.raises(grpc.aio.AioRpcError) as denied:
                await client.ListExecutions(
                    pb.EvaluationExecutionQuery(
                        scope=pb.EvaluationQuery(
                            run_id=str(run_id), organization_id=1, operator_user_id=42
                        ),
                        expected_version=0,
                    )
                )
            assert denied.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert await snapshot(tx, run_id) == before
    finally:
        await server.stop(0)
        await container.close()
