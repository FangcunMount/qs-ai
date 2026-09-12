import json
from datetime import UTC, datetime

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.domain.evaluation.quality_gates import QualityGateResult
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


async def test_gate_preview_uses_trusted_scope_server_time_and_version(handler):
    service, store = handler
    query = pb.EvaluationGateQuery(scope=command().scope, expected_version=12)
    at = datetime.now(UTC)
    store.preview_gates.return_value = GatePreview(
        query.scope.run_id,
        12,
        "sha256:" + "a" * 64,
        QualityGateResult(at, (("G3", True), ("G4", False), ("G5", False)), (), (), ()),
    )
    result = await service.PreviewGates(query, Context())
    trusted, version, evaluated = store.preview_gates.await_args.args
    assert trusted.organization_id == 7 and trusted.actor == "user:42" and version == 12
    assert at <= evaluated <= datetime.now(UTC)
    body = json.loads(result.gate_result_json)
    assert body["schema_version"] == "qs-ai-evaluation-gate-preview/v1"
    assert body["gate_passes"] == {"G1": True, "G2": True, "G3": True, "G4": False, "G5": False}
    assert "approved" not in body and "passed" not in body
    assert pb.EvaluationGatePreview.FromString(result.SerializeToString()) == result


@pytest.mark.parametrize("case", ["scope", "version", "workload"])
async def test_bad_gate_query_is_rejected_before_store(handler, case):
    service, store = handler
    query = pb.EvaluationGateQuery(scope=command().scope, expected_version=12)
    context = Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
    if case == "scope":
        query.scope.organization_id = 0
    elif case == "version":
        query.expected_version = 0
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as caught:
        await service.PreviewGates(query, context)
    assert caught.value.args[0] == expected
    store.preview_gates.assert_not_awaited()


@pytest.mark.parametrize(
    "error,code",
    [
        (CheckpointConflict("private"), grpc.StatusCode.ABORTED),
        (NotFound("private"), grpc.StatusCode.NOT_FOUND),
        (ValueError("private"), grpc.StatusCode.INVALID_ARGUMENT),
    ],
)
async def test_gate_preview_errors_are_redacted(handler, error, code):
    service, store = handler
    store.preview_gates.side_effect = error
    with pytest.raises(Aborted) as caught:
        await service.PreviewGates(
            pb.EvaluationGateQuery(scope=command().scope, expected_version=1), Context()
        )
    assert caught.value.args[0] == code and "private" not in str(caught.value)
