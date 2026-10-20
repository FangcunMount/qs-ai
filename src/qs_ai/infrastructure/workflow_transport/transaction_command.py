"""Owned, authenticated-command input for a host-planned transaction.

This value validates byte/route identity; it does not authenticate a signer.
The transport must authenticate the actual command topic and resolve the exact
body first. No database, clock, signing key or producer belongs in this module.
"""

import hashlib
from dataclasses import dataclass, field

from google.protobuf.message import DecodeError  # type: ignore[import-untyped]
from reliable_messaging.transaction import Request
from reliable_messaging.wire import decode

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.messaging import (
    FIELDS,
    MAX_BODY,
    METADATA,
    MessagingContractError,
    parse_body,
    valid_number,
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
            if not 0 < len(value) <= (262144 if name == "wire" else MAX_BODY):
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
