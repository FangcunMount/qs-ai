"""Protect first business receipt framing before moving it outside the SQL root."""

from uuid import uuid4

import pytest

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    build_receipt_body,
)


def command_fixture():
    return pb.MessagingEnvelope(
        message_id=str(uuid4()), body_sha256="a" * 64, aggregate_key=str(uuid4())
    )


@pytest.mark.parametrize("decision", [pb.ACCEPTED, pb.REJECTED, pb.HELD])
@pytest.mark.parametrize("original", ["workflow", "evaluation", "none"])
def test_original_receipt_preserves_command_correlation_decision_and_scope(decision, original):
    command = command_fixture()
    value = AdmissionDecision(
        "7",
        4,
        decision,
        "known_refusal" if decision != pb.ACCEPTED else "",
        14,
        workflow_receipt=(
            workflow.Receipt(
                session_id=str(uuid4()), run_id=str(uuid4()), version=4, status="blocked"
            )
            if original == "workflow"
            else None
        ),
        evaluation_receipt=(
            workflow.EvaluationState(run_id=str(uuid4()), version=4)
            if original == "evaluation"
            else None
        ),
    )
    receipt = build_receipt_body(command, value).command_receipt
    assert (receipt.command_id, receipt.command_body_sha256) == (
        command.message_id,
        command.body_sha256,
    )
    assert (receipt.decision, receipt.code, receipt.grpc_status_code) == (
        decision,
        value.code,
        value.grpc_status_code,
    )
    if original == "none":
        assert receipt.WhichOneof("original_receipt") is None
    elif original == "workflow":
        assert receipt.workflow_receipt == value.workflow_receipt
        value.workflow_receipt.status = "mutated-after-recording"
        assert receipt.workflow_receipt.status == "blocked", "first body owns its protobuf copy"
    else:
        assert receipt.evaluation_receipt == value.evaluation_receipt


@pytest.mark.parametrize("bad", ["decision", "utf8-code", "grpc-negative", "grpc-large", "both"])
def test_invalid_receipt_never_seals_or_writes_a_decision(bad):
    command = command_fixture()
    values = {"organization_id": "7", "sequence": 1, "decision": pb.ACCEPTED}
    if bad == "decision":
        values["decision"] = pb.MESSAGING_DECISION_UNSPECIFIED
    elif bad == "utf8-code":
        values["code"] = "界" * 43  # 129 bytes, despite fewer than128 code points
    elif bad == "grpc-negative":
        values["grpc_status_code"] = -1
    elif bad == "grpc-large":
        values["grpc_status_code"] = 17
    else:
        values.update(
            workflow_receipt=workflow.Receipt(), evaluation_receipt=workflow.EvaluationState()
        )
    with pytest.raises(ValueError, match="invalid durable admission decision"):
        build_receipt_body(command, AdmissionDecision(**values))
