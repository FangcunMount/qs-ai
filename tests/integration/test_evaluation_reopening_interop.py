"""QS management -> temporary mTLS AI -> MySQL; IAM and model judgments are synthetic."""

import asyncio
import json
import os

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.evaluation.review import CONTRADICTION_POLICY
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_reopening import eligible_semantics as eligible_semantics
from tests.integration.test_evaluation_reopening import rejected_round as rejected_round
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_qs_reopens_resigns_and_finalizes_while_preserving_prior_round(
    rejected_round, go_management, tmp_path
):
    tx, scope, final, _ = rejected_round
    version = final.version
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
            Action="reopen",
            Confirm=True,
            Reason="核对语义判断分歧",
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
        assert (await call(Confirm=False))["Invalid"]
        assert (await call(Version=version - 1))["Code"] == "Aborted"
        assert await rows(tx, scope.run_id) == before
        opened = await call()
        assert opened["Code"] == "OK", opened
        state = opened["State"]
        history = state["review_reopenings"]
        entry = history[0]
        assert entry["actor"] == "user:42" and entry["source_version"] == version
        assert entry["previous_finalization"] == json.loads(final.finalization_json)
        assert entry["previous_reviews"] == json.loads(final.reviews_json)
        assert len(state["reviews"]) == 60 and "finalization" not in state
        assert state["version"] == version + 1
        version = state["version"]
        assert (await call(Action="get", AuditOnly=True, UserID=99))["State"] == state
        assert (await call())["Code"] == "Aborted"
        items = []
        for cid in entry["candidate_ids"]:
            generated = next(r for r in original[0] if r["candidate_id"] == cid)
            candidate = generated["candidate_json"]
            semantic = next(
                r
                for r in original[1]
                if r["execution_id"] == candidate["accepted_semantic_execution_id"]
            )
            assertion = next(
                a
                for a in candidate["semantic_assertions"]
                if a["type"] == "forbidden_claims_absent" and a["scope"] == "default"
            )
            items.append(
                dict(
                    candidate_id=cid,
                    decision="approve",
                    reason="再次审阅原文",
                    semantic_review=dict(
                        policy_version=CONTRADICTION_POLICY,
                        execution_id=semantic["execution_id"],
                        output_fingerprint=semantic["evidence_json"]["output_fingerprint"],
                        assertion_ordinal=assertion["ordinal"],
                        original_detail=assertion["detail"],
                        candidate_excerpt=generated["normalized_output"].decode()[:100],
                        reason="核对原文确认语义分歧",
                    ),
                )
            )
        for uid, role in ((42, "assessment_semantics"), (43, "safety_product")):
            reply = await call(Action="review", UserID=uid, Review=dict(role=role, reviews=items))
            assert reply["Code"] == "OK", reply
            version = reply["State"]["version"]
        preview = await call(Action="gates", AuditOnly=True)
        assert preview["Code"] == "OK" and all(
            preview["State"]["gate_result"]["gate_passes"].values()
        )
        result = await call(Action="finalize", ExpectedPassed=True)
        assert result["Code"] == "OK", result
        assert result["State"]["status"] == "approved"
        assert result["State"]["review_reopenings"] == history
        assert (await call(Action="get", AuditOnly=True))["State"] == result["State"]
        assert await outputs(tx, scope.run_id) == original
    finally:
        await server.stop(None)
        await container.close()
