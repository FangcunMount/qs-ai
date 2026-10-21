"""Freeze only the existing local technical-budget hold, never business retry authority."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from reliable_messaging.transaction import FrozenPlan

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    build_receipt_body,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare, valid_id
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    freeze_admission_plan,
    read_admission_plan,
)

_MAGIC = b"qs-ai-command-technical-hold/v1\0"


def freeze_technical_hold(
    incoming: FrozenCommandInput, signing_key: Any, recipient_key: Any
) -> FrozenPlan:
    operation_id, receipt_id, token = str(uuid4()), str(uuid4()), str(uuid4())
    at = datetime.now(UTC).isoformat(timespec="microseconds")
    envelope = incoming.envelope()
    receipt = prepare(
        pb.COMMAND_RECEIPT,
        receipt_id,
        envelope.aggregate_key,
        build_receipt_body(
            envelope,
            AdmissionDecision(
                incoming.organization_id, 1, pb.HELD, "technical_budget_exhausted", 14
            ),
        ),
        organization_id=incoming.organization_id,
        signing_key=signing_key,
        recipient_key=recipient_key,
        correlation=envelope.message_id,
        original_occurred_at=at,
    )
    plan = freeze_admission_plan(
        incoming, operation_id, ((receipt, 1),), context=_MAGIC + token.encode("ascii")
    )
    read_hold_token(incoming, plan)
    return plan


def read_hold_token(incoming: FrozenCommandInput, plan: FrozenPlan) -> str:
    values = read_admission_plan(incoming, plan)
    if len(values) != 1:
        raise ValueError("Only the original technical hold receipt required")
    value = values[0]
    receipt = pb.MessagingBody.FromString(value.body).command_receipt
    if (
        (receipt.decision, receipt.code, receipt.grpc_status_code)
        != (pb.HELD, "technical_budget_exhausted", 14)
        or not value.context.startswith(_MAGIC)
        or len(value.context) != len(_MAGIC) + 36
    ):
        raise ValueError("Original technical hold context required")
    try:
        token = value.context[len(_MAGIC) :].decode("ascii")
    except UnicodeError:
        raise ValueError("Canonical technical hold reservation required") from None
    if not valid_id(token):
        raise ValueError("Canonical technical hold reservation required")
    return token
