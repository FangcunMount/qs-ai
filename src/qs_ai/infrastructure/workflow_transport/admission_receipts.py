"""Business receipt values, independent of signing, database roots and delivery."""

from dataclasses import dataclass

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow


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
