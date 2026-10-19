"""Frozen local archive codec, independent of signing, network and DB callbacks."""

import struct
from dataclasses import dataclass

from google.protobuf.message import DecodeError  # type: ignore[import-untyped]
from reliable_messaging.message import Message
from reliable_messaging.transaction import FrozenBranch
from reliable_messaging.wire import decode

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.messaging import (
    INLINE_BODY,
    MAX_BODY,
    METADATA,
    SCHEMA,
    MessagingContractError,
    PreparedMessage,
    parse_body,
    route,
    valid_number,
)

CONTENT_TYPE = "application/vnd.qs-ai.archive.v1+octet-stream"
MAGIC = b"qs-ai-archive/v1\0"
LAYOUT = struct.Struct(">QQBII")
MAX_HEADER = INLINE_BODY + 2048


@dataclass(frozen=True)
class FrozenArchive:
    header: bytes
    body: bytes
    wire: bytes
    organization_id: int
    sequence: int
    ordered: bool
    requires_receipt: bool

    def envelope(self) -> pb.MessagingEnvelope:
        value = pb.MessagingEnvelope()
        try:
            value.ParseFromString(self.header)
        except DecodeError:
            raise MessagingContractError("invalid frozen archive header") from None
        return value

    def validate(self) -> None:
        if (
            type(self.organization_id) is not int
            or type(self.sequence) is not int
            or type(self.ordered) is not bool
            or type(self.requires_receipt) is not bool
            or not 0 < self.organization_id <= 2**64 - 1
            or not 0 < self.sequence <= 2**64 - 1
            or not 0 < len(self.header) <= MAX_HEADER
            or not 0 < len(self.body) <= MAX_BODY
            or not 0 < len(self.wire) <= 262144
        ):
            raise MessagingContractError("invalid frozen archive bounds")
        envelope = self.envelope()
        body = parse_body(envelope, self.body)
        org = str(self.organization_id)
        if (
            envelope.HasField("payload_reference")
            and envelope.payload_reference.organization_id != org
        ):
            raise MessagingContractError("archive reference organization differs")
        body_org: str | None = None
        if envelope.kind in (pb.START, pb.CHANGE):
            body_org = str(getattr(body, body.WhichOneof("value")).actor.org_id)
        elif envelope.kind in (pb.PARTICIPANT_RETRY, pb.EVALUATION_START, pb.EVALUATION_CANCEL):
            body_org = str(getattr(body, body.WhichOneof("value")).scope.organization_id)
        elif envelope.kind == pb.INTERPRETATION_STATE:
            body_org = str(body.interpretation_state.actor.org_id)
        elif envelope.kind == pb.EVALUATION_STATE:
            body_org = str(body.evaluation_state.organization_id)
        if body_org is not None and body_org != org:
            raise MessagingContractError("archive body organization differs")
        outer = decode(self.wire)
        if outer.message_id != envelope.message_id or outer.metadata != METADATA:
            raise MessagingContractError("archive first wire identity differs")

    def encode(self) -> bytes:
        self.validate()
        flags = int(self.ordered) | (int(self.requires_receipt) << 1)
        return (
            MAGIC
            + LAYOUT.pack(
                self.organization_id, self.sequence, flags, len(self.header), len(self.body)
            )
            + self.header
            + self.body
        )

    def message(self) -> Message:
        envelope = self.envelope()
        return Message(
            envelope.producer,
            envelope.message_id,
            route(envelope.kind)[2],
            pb.MessagingKind.Name(envelope.kind),
            SCHEMA,
            f"org:{self.organization_id}",
            CONTENT_TYPE,
            envelope.original_occurred_at,
            self.encode(),
        )


def decode_archive(payload: bytes, wire: bytes) -> FrozenArchive:
    prefix = len(MAGIC) + LAYOUT.size
    if len(payload) < prefix or not payload.startswith(MAGIC):
        raise MessagingContractError("invalid archive codec version")
    org, sequence, flags, header_len, body_len = LAYOUT.unpack_from(payload, len(MAGIC))
    if (
        flags > 3
        or not 0 < header_len <= MAX_HEADER
        or not 0 < body_len <= MAX_BODY
        or header_len + body_len != len(payload) - prefix
    ):
        raise MessagingContractError("invalid archive byte lengths")
    result = FrozenArchive(
        bytes(payload[prefix : prefix + header_len]),
        bytes(payload[prefix + header_len :]),
        bytes(wire),
        org,
        sequence,
        bool(flags & 1),
        bool(flags & 2),
    )
    result.validate()
    return result


def freeze_branch(
    prepared: PreparedMessage,
    organization_id: str,
    sequence: int,
    ordered: bool,
    requires_receipt: bool,
    branch_id: str,
    physical_topic: str,
    revision: str,
) -> FrozenBranch:
    if not valid_number(organization_id) or prepared.topic != route(prepared.envelope.kind)[2]:
        raise MessagingContractError("archive original route or organization differs")
    value = FrozenArchive(
        prepared.envelope.SerializeToString(deterministic=True),
        bytes(prepared.body),
        bytes(prepared.wire),
        int(organization_id),
        sequence,
        ordered,
        requires_receipt,
    )
    branch = FrozenBranch(branch_id, physical_topic, revision, value.message(), value.wire)
    decode_branch(branch)
    return branch


def decode_branch(branch: FrozenBranch) -> FrozenArchive:
    value = decode_archive(branch.message.payload, branch.wire)
    if branch.route_revision != "m3-1" or branch.message != value.message():
        raise MessagingContractError("frozen archive message identity differs")
    return value
