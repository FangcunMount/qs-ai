"""Closed Run preview uses real MySQL evidence; model outputs and IAM scope are synthetic."""

import json
import os
from dataclasses import replace
from datetime import timedelta

import grpc
import pytest
from sqlalchemy import delete, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import preview_gates
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_run_policies,
    evaluation_runs,
)
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_reviews import accept, outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


async def test_preview_rebuilds_all_gates_and_never_approves_or_mutates(reviewable):
    tx, scope, version, value = reviewable
    before, original = await rows(tx, scope.run_id), await outputs(tx, scope.run_id)
    store = MySQLEvaluationManagement(tx)
    at = value.reviewed_at + timedelta(seconds=1)
    preview = await store.preview_gates(scope, version, at)
    assert dict(preview.gate_passes) == {
        "G1": True,
        "G2": True,
        "G3": True,
        "G4": False,
        "G5": False,
    }
    assert preview.run_id == str(scope.run_id) and preview.version == version
    assert (
        preview.release_fingerprint
        == json.loads(before[0]["definition_json"])["release_fingerprint"]
    )
    assert await rows(tx, scope.run_id) == before and await outputs(tx, scope.run_id) == original
    batch = tuple(replace(value, candidate_id=f"candidate:{i}") for i in range(1, 36))
    state = await accept(reviewable, batch)
    safety_scope = replace(scope, operator_user_id=43)
    safety = tuple(replace(r, role="safety_product", reviewer=safety_scope.actor) for r in batch)
    state = await accept(reviewable, safety, version=state.version, scope=safety_scope)
    reviewed = await store.preview_gates(scope, state.version, at)
    # This shared fixture deliberately reuses one output across all cases;
    # complete human approval must not erase its case-quality failures.
    assert dict(reviewed.gate_passes) == {
        "G1": True,
        "G2": True,
        "G3": True,
        "G4": False,
        "G5": True,
    }
    after = await rows(tx, scope.run_id)
    assert after[0]["progress_json"]["status"] == "awaiting_review"
    assert after[2]["version"] == state.version
    assert await outputs(tx, scope.run_id) == original
    with pytest.raises(CheckpointConflict):
        await store.preview_gates(scope, version, at)


async def test_preview_scope_is_checked_before_version_and_details(reviewable):
    tx, scope, version, value = reviewable
    store = MySQLEvaluationManagement(tx)
    with pytest.raises(NotFound):
        await store.preview_gates(
            replace(scope, organization_id=2), version + 10, value.reviewed_at
        )
    with pytest.raises(ValueError):
        await store.preview_gates(scope, version, value.reviewed_at - timedelta(seconds=1))


@pytest.mark.parametrize(
    "case",
    [
        "release",
        "suite",
        "slots",
        "audit",
        "audit_reason",
        "start_reason",
        "policy",
        "preflight",
        "history",
        "unknown_count",
        "dispatch",
    ],
)
async def test_corrupt_frozen_or_closed_evidence_cannot_produce_a_gate_preview(reviewable, case):
    tx, scope, version, value = reviewable
    before = await rows(tx, scope.run_id)
    creation = json.loads(before[0]["definition_json"])
    progress = json.loads(json.dumps(before[0]["progress_json"]))
    if case == "release":
        creation["release_fingerprint"] = "sha256:" + "0" * 64
    elif case == "suite":
        creation["suite_json"] = "{}"
    elif case == "slots":
        creation["slots"][0]["case_id"] = "unregistered"
    elif case == "audit":
        creation["audit"]["organization_id"] = 2
    elif case == "audit_reason":
        creation["audit"]["request_reason"] = " "
    elif case == "start_reason":
        progress["transitions"][1]["reason"] = " "
    elif case == "preflight":
        progress["preflight"]["assertions"][0]["detail"] = "篡改"
    elif case == "history":
        progress["transitions"][1]["from"] = "blocked"
    elif case == "unknown_count":
        progress["unresolved_result_unknown_count"] = 1
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(scope.run_id))
            .values(definition_json=json.dumps(creation), progress_json=progress)
        )
        if case == "policy":
            await db.execute(
                update(evaluation_run_policies)
                .where(evaluation_run_policies.c.run_id == str(scope.run_id))
                .values(fingerprint="sha256:" + "0" * 64)
            )
        if case == "dispatch":
            await db.execute(
                delete(evaluation_dispatches).where(
                    evaluation_dispatches.c.run_id == str(scope.run_id)
                )
            )
        await db.commit()
    corrupted = await rows(tx, scope.run_id)
    async with tx.open() as db:
        with pytest.raises((ValueError, CheckpointConflict)):
            await preview_gates(db, scope, version, value.reviewed_at)
    assert await rows(tx, scope.run_id) == corrupted


async def test_preview_snapshot_cannot_mix_a_concurrent_review_version(reviewable, monkeypatch):
    from qs_ai.infrastructure.persistence.mysql import evaluation_gates

    tx, scope, version, value = reviewable
    original_header = evaluation_gates.header

    async def concurrent_review(db, requested_scope):
        header = await original_header(db, requested_scope)
        await accept(reviewable)
        return header

    monkeypatch.setattr(evaluation_gates, "header", concurrent_review)
    store = MySQLEvaluationManagement(tx)
    result = await store.preview_gates(scope, version, value.reviewed_at)
    assert result.version == version
    assert sum(r.code == "human_review_incomplete" for r in result.quality.reasons) == 70
    after = await rows(tx, scope.run_id)
    assert after[2]["version"] == version + 1
    assert len(after[0]["progress_json"]["human_reviews"]) == 1


async def test_gate_preview_over_mtls_reuses_scope_and_original_mysql_evidence(
    reviewable, tmp_path
):
    tx, scope, version, _ = reviewable
    before = await rows(tx, scope.run_id)
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
    try:
        for identity in ("qs", "other"):
            credentials = grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            )
            async with grpc.aio.secure_channel(f"localhost:{port}", credentials) as channel:
                stub = rpc.EvaluationManagementStub(channel)
                query = pb.EvaluationGateQuery(
                    scope=pb.EvaluationQuery(
                        run_id=str(scope.run_id), organization_id=1, operator_user_id=42
                    ),
                    expected_version=version,
                )
                if identity == "other":
                    with pytest.raises(grpc.aio.AioRpcError) as caught:
                        await stub.PreviewGates(query, timeout=5)
                    assert caught.value.code() == grpc.StatusCode.PERMISSION_DENIED
                    continue
                result = await stub.PreviewGates(query, timeout=5)
                assert result.version == version
                assert json.loads(result.gate_result_json)["gate_passes"] == {
                    "G1": True,
                    "G2": True,
                    "G3": True,
                    "G4": False,
                    "G5": False,
                }
                query.scope.organization_id = 2
                with pytest.raises(grpc.aio.AioRpcError) as caught:
                    await stub.PreviewGates(query, timeout=5)
                assert caught.value.code() == grpc.StatusCode.NOT_FOUND
        assert await rows(tx, scope.run_id) == before
    finally:
        await server.stop(None)
        await container.close()
