"""Actual QS application/client -> mTLS Python management -> MySQL review acceptance."""

import asyncio
import json
import os
from datetime import UTC, datetime

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_review_authority_batch_roundtrip_and_readback(reviewable, go_management, tmp_path):
    tx, scope, version, _ = reviewable
    run_id = scope.run_id
    original = await outputs(tx, run_id)
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
    ca, cert, key = [(tmp_path / n).read_bytes() for n in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    async def call(certificate="qs", **changes):
        request = dict(
            RunID=str(run_id),
            Action="review",
            OrgID=1,
            Version=version,
            Allowed=True,
            Review={
                "role": "assessment_semantics",
                "reviews": [
                    {"candidate_id": "candidate:1", "decision": "approve", "reason": " 核对事实 "},
                    {"candidate_id": "candidate:2", "decision": "reject", "reason": "保留拒绝意见"},
                ],
            },
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
        before = await rows(tx, run_id)
        assert (await call(Allowed=False))["Denied"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert (await call(OrgID=2))["Code"] == "NotFound"
        assert (await call(Review={"role": "admin", "reviews": []}))["Invalid"]
        assert await rows(tx, run_id) == before
        at = datetime.now(UTC)
        result = await call()
        assert result["Code"] == "OK" and result["State"]["version"] == version + 1
        reviews = result["State"]["reviews"]
        assert len(reviews) == 2
        assert {r["reviewer"] for r in reviews} == {"user:42"}
        assert reviews[0]["reason"] == "核对事实" and reviews[1]["decision"] == "reject"
        assert at <= datetime.fromisoformat(reviews[0]["reviewed_at"]) <= datetime.now(UTC)
        assert reviews[0]["reviewed_at"] == reviews[1]["reviewed_at"]
        assert (await call(Action="get"))["State"] == result["State"]
        assert (await call())["Code"] == "Aborted"
        assert (await call(Version=version + 1))["Code"] == "InvalidArgument"
        safety = {
            "role": "safety_product",
            "reviews": [
                {"candidate_id": "candidate:1", "decision": "approve", "reason": "安全复核完成"}
            ],
        }
        assert (await call(Version=version + 1, Review=safety))["Code"] == "InvalidArgument"
        accepted = await call(Version=version + 1, UserID=43, Review=safety)
        assert accepted["Code"] == "OK" and accepted["State"]["version"] == version + 2
        assert accepted["State"]["reviews"][-1]["reviewer"] == "user:43"
        assert (await call(Version=version + 2, Allowed=False))["Denied"]
        assert (await call(Action="get"))["State"] == accepted["State"]
        assert await outputs(tx, run_id) == original
    finally:
        await server.stop(None)
        await container.close()
