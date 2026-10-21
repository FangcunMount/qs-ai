"""Owned, authenticated-command input for a host-planned transaction.

This value validates byte/route identity; it does not authenticate a signer.
The transport must authenticate the actual command topic and resolve the exact
body first. No database, clock, signing key or producer belongs in this module.
"""

import hashlib
from dataclasses import dataclass, field

from google.protobuf.message import DecodeError  # type: ignore[import-untyped]
from reliable_messaging.transaction import FrozenPlan, Request
from reliable_messaging.transport import physical_topic_v2
from reliable_messaging.wire import decode

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    FIELDS,
    MAX_BODY,
    METADATA,
    MessagingContractError,
    PreparedMessage,
    parse_body,
    valid_number,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import (
    FrozenArchive,
    decode_branch,
    freeze_branch,
)

_COMMAND_KINDS = (
    pb.START,
    pb.CHANGE,
    pb.PARTICIPANT_RETRY,
    pb.EVALUATION_START,
    pb.EVALUATION_CANCEL,
)
_INPUT_DOMAIN = b"qs-ai-incoming-command/v1\0"


def _organization(envelope: pb.MessagingEnvelope, body: pb.MessagingBody) -> str:
    value = getattr(body, FIELDS[envelope.kind])
    org = (
        value.actor.org_id
        if envelope.kind in (pb.START, pb.CHANGE)
        else str(value.scope.organization_id)
    )
    if not valid_number(org) or (
        envelope.HasField("payload_reference") and envelope.payload_reference.organization_id != org
    ):
        raise MessagingContractError("original command organization differs")
    return str(org)


@dataclass(frozen=True)
class FrozenCommandInput:
    header: bytes = field(repr=False)
    body: bytes = field(repr=False)
    wire: bytes = field(repr=False)

    def __post_init__(self) -> None:
        # Own even a caller's bytearray/memoryview; never retain mutable protobuf.
        for name in ("header", "body", "wire"):
            value = getattr(self, name)
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise MessagingContractError("original command bytes required")
            if not 0 < memoryview(value).nbytes <= (262144 if name == "wire" else MAX_BODY):
                raise MessagingContractError("original command byte bounds exceeded")
            object.__setattr__(self, name, bytes(value))
        envelope = self.envelope()
        if envelope.kind not in _COMMAND_KINDS:
            raise MessagingContractError("original command kind required")
        body = parse_body(envelope, self.body)
        _organization(envelope, body)
        try:
            outer = decode(self.wire)
        except (ValueError, TypeError):
            raise MessagingContractError("original command wire invalid") from None
        if outer.message_id != envelope.message_id or outer.metadata != METADATA:
            raise MessagingContractError("original command wire identity differs")

    @classmethod
    def from_authenticated(
        cls, envelope: pb.MessagingEnvelope, body: bytes, wire: bytes
    ) -> "FrozenCommandInput":
        return cls(envelope.SerializeToString(deterministic=True), body, wire)

    def envelope(self) -> pb.MessagingEnvelope:
        envelope = pb.MessagingEnvelope()
        try:
            envelope.ParseFromString(self.header)
        except DecodeError:
            raise MessagingContractError("original command header invalid") from None
        return envelope

    def body_message(self) -> pb.MessagingBody:
        return parse_body(self.envelope(), self.body)

    @property
    def organization_id(self) -> str:
        envelope = self.envelope()
        return _organization(envelope, parse_body(envelope, self.body))

    @property
    def request(self) -> Request:
        envelope = self.envelope()
        org = self.organization_id
        # Canonical decimal organization + NUL, then three fixed-width hashes.
        # First receipt IDs/time/ciphertext are intentionally absent. On replay
        # the original signed incoming wire is retained, never reconstructed.
        digest = hashlib.sha256(
            _INPUT_DOMAIN
            + org.encode()
            + b"\0"
            + hashlib.sha256(self.header).digest()
            + bytes.fromhex(envelope.body_sha256)
            + hashlib.sha256(self.wire).digest()
        ).digest()
        return Request(
            "ai.mysql", f"org:{org}", f"ai-command/qs-server/{envelope.message_id}", digest
        )


def freeze_admission_plan(
    incoming: FrozenCommandInput,
    operation_id: str,
    publications: tuple[tuple[PreparedMessage, int], ...],
    *,
    context: bytes = b"",
) -> FrozenPlan:
    """Freeze already prepared first messages; planning/signing happens at the host.

    Each publication carries its business sequence, not a Broker/FIFO promise.
    The v1 archive remains byte-compatible. This function never creates IDs,
    clocks, connections, half messages or a local executor.
    """
    physical = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    request = incoming.request
    branches = tuple(
        freeze_branch(
            prepared,
            incoming.organization_id,
            sequence,
            False,
            True,
            prepared.envelope.message_id,
            physical,
            "m3-1",
            context=context,
        )
        for prepared, sequence in publications
    )
    plan = FrozenPlan(
        request.owner, request.scope, operation_id, request.key, request.input_hash, branches
    )
    read_admission_plan(incoming, plan)
    return plan


def read_admission_plan(
    incoming: FrozenCommandInput, plan: FrozenPlan
) -> tuple[FrozenArchive, ...]:
    """Read the durable winner, binding every first wire to the original command."""
    incoming.request.match(plan)
    physical = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    command = incoming.envelope()
    expected_state = (
        pb.EVALUATION_STATE
        if command.kind in (pb.EVALUATION_START, pb.EVALUATION_CANCEL)
        else pb.INTERPRETATION_STATE
    )
    archives: list[FrozenArchive] = []
    receipts: list[pb.MessagingCommandReceipt] = []
    states: list[tuple[FrozenArchive, pb.MessagingBody]] = []
    for branch in plan.branches:
        value = decode_branch(branch)
        envelope = value.envelope()
        if (
            branch.physical_topic != physical
            or branch.branch_id != envelope.message_id
            or str(value.organization_id) != incoming.organization_id
            or value.ordered
            or not value.requires_receipt
            or envelope.producer != "qs-ai"
            or envelope.destination != "qs-server"
            or envelope.aggregate_key != command.aggregate_key
        ):
            raise MessagingContractError("frozen admission route or identity differs")
        body = parse_body(envelope, value.body)
        if envelope.kind == pb.COMMAND_RECEIPT:
            receipt = body.command_receipt
            if (
                receipt.command_id != command.message_id
                or receipt.command_body_sha256 != command.body_sha256
                or len(receipt.code.encode()) > 128
                or not 0 <= receipt.grpc_status_code <= 16
            ):
                raise MessagingContractError("frozen admission receipt differs")
            receipts.append(receipt)
        elif envelope.kind != expected_state or envelope.correlation_command_id:
            raise MessagingContractError("frozen admission state differs")
        else:
            states.append((value, body))
        archives.append(value)
    if len(receipts) != 1 or (receipts[0].decision == pb.HELD and len(archives) != 1):
        raise MessagingContractError("one original receipt and no held business state required")
    for value, body in states:
        _check_state_receipt(value, body, receipts[0], command.aggregate_key)
    if len({value.context for value in archives}) != 1:
        raise MessagingContractError("frozen admission local contexts differ")
    return tuple(archives)


def _check_state_receipt(
    value: FrozenArchive,
    body: pb.MessagingBody,
    receipt: pb.MessagingCommandReceipt,
    aggregate: str,
) -> None:
    if body.HasField("interpretation_state"):
        state = body.interpretation_state
        original = receipt.workflow_receipt
        if (
            not receipt.HasField("workflow_receipt")
            or state.request_id != aggregate
            or state.session_id != original.session_id
            or state.version != original.version
            or state.status != original.status
            or value.sequence != state.version
        ):
            raise MessagingContractError("frozen interpretation state and receipt differ")
    else:
        evaluation = body.evaluation_state
        original_evaluation = receipt.evaluation_receipt
        if (
            not receipt.HasField("evaluation_receipt")
            or evaluation.run_id != aggregate
            or evaluation.run_id != original_evaluation.run_id
            or evaluation.version != original_evaluation.version
            or evaluation.status != original_evaluation.status
            or value.sequence != evaluation.event_sequence
        ):
            raise MessagingContractError("frozen evaluation state and receipt differ")
