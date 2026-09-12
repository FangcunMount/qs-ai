"""QS application -> real temporary mTLS AI server -> MySQL; no real IAM/model claim."""

import asyncio
import json
import os

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_finalization import passing_reviewable as passing_reviewable
from tests.integration.test_evaluation_finalization import passing_semantics as passing_semantics
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def exercise(context, go_management, tmp_path, passed):
    tx, scope, version, _ = context
    original = await outputs(tx, scope.run_id)
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
            RunID=str(scope.run_id),
            OrgID=1,
            Version=version,
            Allowed=True,
            Action="finalize",
            ExpectedPassed=passed,
            Confirm=True,
            Reason="完整审核后确认",
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
        before = await rows(tx, scope.run_id)
        assert (await call(AuditOnly=True))["Denied"]
        assert (await call(Allowed=False))["Denied"]
        assert (await call(OrgID=2))["Code"] == "NotFound"
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert (await call(ExpectedPassed=None))["Invalid"]
        assert (await call(Confirm=False))["Invalid"]
        assert (await call())["Code"] == "InvalidArgument"  # 70 reviews are not yet present.
        assert await rows(tx, scope.run_id) == before
        for user, role in ((42, "assessment_semantics"), (43, "safety_product")):
            review = {
                "role": role,
                "reviews": [
                    {"candidate_id": f"candidate:{i}", "decision": "approve", "reason": "逐条核对"}
                    for i in range(1, 36)
                ],
            }
            accepted = await call(Action="review", UserID=user, Review=review)
            assert accepted["Code"] == "OK"
            version = accepted["State"]["version"]
        preview = await call(Action="gates", AuditOnly=True)
        assert preview["Code"] == "OK"
        assert all(preview["State"]["gate_result"]["gate_passes"].values()) is passed
        assert (await call(ExpectedPassed=not passed))["Code"] == "Aborted"
        final = await call()
        assert final["Code"] == "OK", final
        result = final["State"]
        assert result["version"] == version + 1
        assert result["status"] == ("approved" if passed else "rejected")
        receipt = result["finalization"]
        assert receipt["actor"] == "user:42" and receipt["passed"] is passed
        assert receipt["source_version"] == version
        assert receipt["release_fingerprint"] == preview["State"]["release_fingerprint"]
        assert (await call(Action="get", AuditOnly=True, UserID=99))["State"] == result
        assert (await call())["Code"] == "Aborted"
        assert (await call(Version=version + 1))["Code"] == "Aborted"
        assert await outputs(tx, scope.run_id) == original
    finally:
        await server.stop(None)
        await container.close()


async def test_qs_finalizes_rejection_and_reads_audited_state(reviewable, go_management, tmp_path):
    await exercise(reviewable, go_management, tmp_path, False)


async def test_qs_finalizes_approval_and_reads_audited_state(
    passing_reviewable, go_management, tmp_path
):
    await exercise(passing_reviewable, go_management, tmp_path, True)
