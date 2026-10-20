"""Business receipt values, independent of signing, database roots and delivery."""

from dataclasses import asdict, dataclass

from qs_ai.application.interpretation.ports import Receipt
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.contracts.workflow.command_mapping import receipt_message
from qs_ai.domain.interpretation.model import Actor


@dataclass(frozen=True)
class AdmissionDecision:
    organization_id: str
    sequence: int
    decision: pb.MessagingDecision
    code: str = ""
    grpc_status_code: int = 0
    workflow_receipt: workflow.Receipt | None = None
    evaluation_receipt: workflow.EvaluationState | None = None


def build_receipt_body(
    envelope: pb.MessagingEnvelope, result: AdmissionDecision
) -> pb.MessagingBody:
    """Copy the original business result; never generate identity/time or perform I/O.

    Both the historical receiver and the planned transaction entry use this same
    contract. The caller chooses and freezes identity/time before signing; this
    function does not infer commit, start a transaction or own a mutable result.
    """
    if (
        result.decision not in (pb.ACCEPTED, pb.REJECTED, pb.HELD)
        or len(result.code.encode()) > 128
        or not 0 <= result.grpc_status_code <= 16
        or (result.workflow_receipt is not None and result.evaluation_receipt is not None)
    ):
        raise ValueError("invalid durable admission decision")
    receipt = pb.MessagingCommandReceipt(
        command_id=envelope.message_id,
        command_body_sha256=envelope.body_sha256,
        decision=result.decision,
        code=result.code,
        grpc_status_code=result.grpc_status_code,
    )
    if result.workflow_receipt is not None:
        receipt.workflow_receipt.CopyFrom(result.workflow_receipt)
    if result.evaluation_receipt is not None:
        receipt.evaluation_receipt.CopyFrom(result.evaluation_receipt)
    return pb.MessagingBody(command_receipt=receipt)


def start_decision(
    request: workflow.StartCommand, receipt: Receipt, org: str, event: object = None
) -> AdmissionDecision:
    """Use exact original refusal evidence; this value is never a commit proof."""
    code, status = "", 0
    decision = pb.ACCEPTED
    if receipt.status == "blocked":
        statuses = {
            "configuration_unavailable": 9,
            "admission_configuration_invalid": 9,
            "admission_input_invalid": 3,
            "participant_daily_capacity_exceeded": 8,
        }
        if (
            not isinstance(event, dict)
            or event.get("session_id") != receipt.session_id
            or event.get("request_id") != request.request_id
            or event.get("version") != receipt.version
            or event.get("status") != "blocked"
            or event.get("actor") != asdict(Actor(org, request.actor.subject_id))
            or not isinstance(event.get("failure_code"), str)
            or event["failure_code"] not in statuses
        ):
            # Missing/corrupt durable evidence is technical Unknown, never a new
            # business rejection or acceptance. The receiver retains REQ/hold rules.
            raise RuntimeError("Original admission refusal evidence unavailable")
        code = event["failure_code"]
        status, decision = statuses[code], pb.REJECTED
    return AdmissionDecision(
        org,
        max(1, receipt.version),
        decision,
        code,
        status,
        workflow_receipt=receipt_message(receipt),
    )
