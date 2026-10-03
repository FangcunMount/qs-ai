"""Authenticated MQ framing; no business dispatch, commits or hidden connections."""

import hashlib
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from google.protobuf.message import DecodeError  # type: ignore[import-untyped]
from reliable_messaging.protected import Context, TrustedSigner, open_message, seal
from reliable_messaging.wire import Envelope, decode, encode, fits_nsq

from qs_ai.contracts.workflow import messaging_pb2 as pb

SCHEMA = "qs-ai-messaging/v1"
COMMANDS = "qs.ai.commands.v1"
EVENTS = "qs.ai.events.v1"
ACKS = "qs.ai.acks.v1"
MAX_BODY = 16 * 1024 * 1024 - 1  # MEDIUMBLOB capacity; existing business limits still apply.
# Conservative pre-seal choice covers JOSE, Revision 2 and worst failure-handoff overhead.
# Oversized bodies are referenced before signing, so each message is sealed only once.
INLINE_BODY = 32 * 1024
METADATA = {"secure_profile": "rm-secure-v1"}
CHANNELS = {COMMANDS: "qs-ai.commands.v1", EVENTS: "qs-server.ai-events.v1", ACKS: "qs-ai.acks.v1"}
FIELDS = {
    pb.START: "start",
    pb.CHANGE: "change",
    pb.PARTICIPANT_RETRY: "participant_retry",
    pb.EVALUATION_START: "evaluation_start",
    pb.EVALUATION_CANCEL: "evaluation_cancel",
    pb.COMMAND_RECEIPT: "command_receipt",
    pb.INTERPRETATION_STATE: "interpretation_state",
    pb.EVALUATION_STATE: "evaluation_state",
    pb.EVENT_ACKNOWLEDGEMENT: "event_acknowledgement",
}


class MessagingContractError(ValueError):
    """Fixed public classification; never contains untrusted body or key data."""


def valid_id(value: str) -> bool:
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def valid_number(value: str) -> bool:
    return bool(re.fullmatch(r"[1-9][0-9]{0,19}", value)) and int(value) <= 2**64 - 1


def valid_hash(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", value))


def route(kind: int) -> tuple[str, str, str]:
    if kind in (
        pb.START,
        pb.CHANGE,
        pb.PARTICIPANT_RETRY,
        pb.EVALUATION_START,
        pb.EVALUATION_CANCEL,
    ):
        return "qs-server", "qs-ai", COMMANDS
    if kind in (pb.COMMAND_RECEIPT, pb.INTERPRETATION_STATE, pb.EVALUATION_STATE):
        return "qs-ai", "qs-server", EVENTS
    if kind == pb.EVENT_ACKNOWLEDGEMENT:
        return "qs-server", "qs-ai", ACKS
    raise MessagingContractError("unsupported messaging kind")


def validate_header(envelope: pb.MessagingEnvelope, topic: str) -> None:
    producer, destination, expected_topic = route(envelope.kind)
    valid = (
        envelope.schema_version == SCHEMA
        and valid_id(envelope.message_id)
        and 0 < len(envelope.aggregate_key.encode()) <= 192
        and 0 < envelope.body_length <= MAX_BODY
        and valid_hash(envelope.body_sha256)
        and (envelope.producer, envelope.destination, topic)
        == (producer, destination, expected_topic)
        and len(envelope.original_occurred_at.encode()) <= 64
    )
    body_kind = envelope.WhichOneof("body")
    if body_kind == "inline_body":
        valid = valid and len(envelope.inline_body) == envelope.body_length
    elif body_kind == "payload_reference":
        r = envelope.payload_reference
        valid = valid and (
            r.producer == producer
            and r.destination == destination
            and r.message_id == envelope.message_id
            and r.body_sha256 == envelope.body_sha256
            and r.body_length == envelope.body_length
            and valid_number(r.organization_id)
        )
    else:
        valid = False
    if not valid:
        raise MessagingContractError("invalid messaging header")


def parse_body(envelope: pb.MessagingEnvelope, raw: bytes) -> pb.MessagingBody:
    validate_header(envelope, route(envelope.kind)[2])
    if len(raw) != envelope.body_length or hashlib.sha256(raw).hexdigest() != envelope.body_sha256:
        raise MessagingContractError("original body mismatch")
    body = pb.MessagingBody()
    try:
        body.ParseFromString(raw)
    except DecodeError:
        raise MessagingContractError("invalid messaging body") from None
    if body.WhichOneof("value") != FIELDS[envelope.kind]:
        raise MessagingContractError("messaging kind/body mismatch")
    value = getattr(body, FIELDS[envelope.kind])
    valid = True
    if envelope.kind == pb.START:
        valid = value.request_id == envelope.message_id
    elif envelope.kind in (pb.CHANGE, pb.PARTICIPANT_RETRY):
        valid = value.command_id == envelope.message_id
    elif envelope.kind == pb.EVALUATION_CANCEL:
        valid = value.HasField("discard")
    elif envelope.kind == pb.COMMAND_RECEIPT:
        valid = (
            valid_id(value.command_id)
            and value.command_id == envelope.correlation_command_id
            and valid_hash(value.command_body_sha256)
            and value.decision in (pb.ACCEPTED, pb.REJECTED, pb.HELD)
        )
    elif envelope.kind == pb.INTERPRETATION_STATE:
        valid = value.event_id == envelope.message_id
    elif envelope.kind == pb.EVALUATION_STATE:
        valid = (
            valid_id(value.run_id)
            and valid_number(value.organization_id)
            and value.event_sequence > 0
        )
    elif envelope.kind == pb.EVENT_ACKNOWLEDGEMENT:
        valid = (
            valid_id(value.event_id)
            and valid_hash(value.event_body_sha256)
            and value.event_kind
            in (pb.COMMAND_RECEIPT, pb.INTERPRETATION_STATE, pb.EVALUATION_STATE)
            and value.outcome
            in (
                pb.MessagingEventAcknowledgement.STORED,
                pb.MessagingEventAcknowledgement.TECHNICALLY_HELD,
            )
        )
    if not valid:
        raise MessagingContractError("invalid original message identity or decision")
    return body


@dataclass(frozen=True)
class PreparedMessage:
    envelope: pb.MessagingEnvelope
    body: bytes
    wire: bytes
    topic: str


def prepare(
    kind: pb.MessagingKind,
    identity: str,
    aggregate: str,
    body: pb.MessagingBody,
    *,
    organization_id: str,
    signing_key: Any,
    recipient_key: Any,
    correlation: str = "",
    original_occurred_at: str = "",
) -> PreparedMessage:
    producer, destination, topic = route(kind)
    raw = body.SerializeToString(deterministic=True)
    envelope = pb.MessagingEnvelope(
        schema_version=SCHEMA,
        producer=producer,
        destination=destination,
        message_id=identity,
        kind=kind,
        aggregate_key=aggregate,
        correlation_command_id=correlation,
        body_sha256=hashlib.sha256(raw).hexdigest(),
        body_length=len(raw),
        original_occurred_at=original_occurred_at,
    )
    if len(raw) <= INLINE_BODY:
        envelope.inline_body = raw
    else:
        envelope.payload_reference.CopyFrom(
            pb.MessagePayloadReference(
                producer=producer,
                destination=destination,
                message_id=identity,
                body_sha256=envelope.body_sha256,
                body_length=len(raw),
                organization_id=organization_id,
            )
        )
    parse_body(envelope, raw)
    context = Context(producer, destination, topic, identity, METADATA)
    sealed = seal(
        context, envelope.SerializeToString(deterministic=True), signing_key, recipient_key
    )
    framed = Envelope(identity, sealed, METADATA)
    if not fits_nsq(framed, topic, CHANNELS[topic]):
        raise MessagingContractError("protected wire exceeds failure handoff budget")
    return PreparedMessage(envelope, raw, encode(framed), topic)


def authenticate(
    wire: bytes,
    topic: str,
    *,
    decrypt_keys: dict[str, Any],
    trusted_signers: dict[str, TrustedSigner],
) -> pb.MessagingEnvelope:
    if topic not in CHANNELS or len(wire) > 262_144:
        raise MessagingContractError("invalid topic or wire size")
    outer = decode(wire)
    if not valid_id(outer.message_id) or outer.metadata != METADATA:
        raise MessagingContractError("invalid protected envelope identity")
    producer, destination = ("qs-ai", "qs-server") if topic == EVENTS else ("qs-server", "qs-ai")
    payload = open_message(
        outer.payload,
        Context(producer, destination, topic, outer.message_id, outer.metadata),
        decrypt_keys=decrypt_keys,
        trusted_signers=trusted_signers,
        max_payload_bytes=INLINE_BODY + 2048,
    )
    envelope = pb.MessagingEnvelope()
    try:
        envelope.ParseFromString(payload)
    except DecodeError:
        raise MessagingContractError("invalid authenticated messaging envelope") from None
    validate_header(envelope, topic)
    if envelope.message_id != outer.message_id:
        raise MessagingContractError("protected identity mismatch")
    return envelope
