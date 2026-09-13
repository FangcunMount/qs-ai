"""QS app/client -> temporary mTLS -> AI/MySQL; synthetic IAM and model evidence."""

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
from tests.integration.test_evaluation_cancellation import prepared
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_reopening import eligible_semantics as eligible_semantics
from tests.integration.test_evaluation_reopening import open_round
from tests.integration.test_evaluation_reopening import rejected_round as rejected_round
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_start import requested as requested
from tests.integration.test_semantic_completions import judge as judge

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def verify(tx, scope, version, discard, go_management, tmp_path):
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
            Action="cancel",
            Confirm=True,
            Reason="停止后续评测",
            Discard=discard,
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
        evidence = await outputs(tx, scope.run_id)
        previous = (await call(Action="get", AuditOnly=True))["State"]
        candidate = (
            await call(Action="candidate", CandidateID="candidate:1", AuditOnly=True)
            if discard
            else None
        )
        assert (await call(AuditOnly=True))["Denied"]
        assert (await call(Allowed=False))["Denied"]
        assert (await call(UserID=-1))["Invalid"]
        assert (await call(Discard=None))["Invalid"]
        assert (await call(Confirm=False))["Invalid"]
        assert (await call(Version=version + 10))["Code"] == "Aborted"
        assert (await call(OrgID=2))["Code"] == "NotFound"
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert await rows(tx, scope.run_id) == before
        reply = await call()
        assert reply["Code"] == "OK", reply
        state = reply["State"]
        receipt = state["cancellation"]
        assert receipt["source_version"] == version and receipt["version"] == version + 1
        assert receipt["actor"] == "user:42" and receipt["discard"] is discard
        assert receipt["release_fingerprint"] == previous["creation"]["release_fingerprint"]
        assert state["reviews"] == previous["reviews"]
        assert state["review_reopenings"] == previous["review_reopenings"]
        assert state["status"] == "canceled"
        assert (await call(Action="get", AuditOnly=True, UserID=99))["State"] == state
        assert (await call())["Code"] == "Aborted"
        version = state["version"]
        assert (await call())["Code"] == "Aborted"
        assert (await call(Action="start"))["Code"] == "Aborted"
        if candidate:
            current = await call(Action="candidate", CandidateID="candidate:1", AuditOnly=True)
            assert current["Code"] == candidate["Code"] == "OK"
            current["State"]["version"] = candidate["State"]["version"]
            assert current == candidate
        assert await outputs(tx, scope.run_id) == evidence
    finally:
        await server.stop(0)
        await container.close()


@pytest.mark.parametrize("prepare", [False, True])
async def test_qs_cancel_requested_or_prepared_preserves_original_work(
    requested, go_management, tmp_path, prepare
):
    tx, scope, _ = requested
    version = (await prepared(requested)).version if prepare else 1
    await verify(tx, scope, version, False, go_management, tmp_path)


async def test_qs_discard_keeps_original_reviews_and_candidate_readback(
    rejected_round, go_management, tmp_path
):
    tx, scope, *_ = rejected_round
    state = await open_round(rejected_round)
    await verify(tx, scope, state.version, True, go_management, tmp_path)
