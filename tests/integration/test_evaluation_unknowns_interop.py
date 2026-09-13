"""Actual QS adapter -> temporary mTLS -> AI/MySQL; synthetic IAM and provider evidence."""

import asyncio
import json
import os
from dataclasses import asdict

import grpc
import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.evaluation_unknowns import list_unknowns
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_recovery import pending, recover
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.mark.parametrize("semantic", [False, True])
async def test_qs_unknown_query_scope_snapshot_and_original_resolution(
    ready, go_management, tmp_path, semantic
):
    tx, run_id, *_ = ready
    version = (await step(ready, Gateway(ready))).version if semantic else 3
    state = await recover(ready, await pending(ready, dispatched=True, version=version))
    version = state.version
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
            RunID=str(run_id),
            OrgID=1,
            Version=version,
            Allowed=True,
            AuditOnly=True,
            Action="unknowns",
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
            raw, _ = await asyncio.wait_for(process.communicate(json.dumps(request).encode()), 15)
            assert process.returncode == 0
            return json.loads(raw)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    try:
        before = await snapshot(tx, run_id)
        async with tx.open() as db:
            expected = await list_unknowns(db, ManagementScope(run_id, 1, 42), version)
        response = await call()
        assert response["Code"] == "OK", response
        assert response["State"] == json.loads(json.dumps(asdict(expected)))
        view = response["State"]
        target = view["executions"][0]
        assert target["kind"] == ("semantic" if semantic else "generation")
        assert bool(target["candidate_id"]) is semantic
        assert target["replacement_allowed"] and view["can_resolve"]
        assert (await call(Allowed=False))["Denied"]
        assert (await call(UserID=-1))["Invalid"]
        assert (await call(Version=0))["Invalid"]
        assert (await call(Version=version - 1))["Code"] == "Aborted"
        assert (await call(OrgID=2))["Code"] == "NotFound"
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert (await call(Action="resolve", Decision="cancel_run", Confirm=True))["Denied"]
        assert await snapshot(tx, run_id) == before
        resolved = await call(
            Action="resolve",
            AuditOnly=False,
            ExecutionID=target["execution_id"],
            Decision="cancel_run",
            Confirm=True,
        )
        assert resolved["Code"] == "OK", resolved
        assert resolved["State"]["resolutions"][0]["execution_id"] == target["execution_id"]
        assert (await call())["Code"] == "Aborted"
        version = resolved["State"]["version"]
        current = await call()
        assert current["Code"] == "OK", current
        assert current["State"]["status"] == "canceled"
        assert current["State"]["executions"] == [] and not current["State"]["can_resolve"]
        assert (await snapshot(tx, run_id))[1] == before[1]
    finally:
        await server.stop(0)
        await container.close()
