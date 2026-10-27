"""Input ownership/identity only; opaque unit wire does not claim authentication."""

import hashlib
from dataclasses import replace
from uuid import uuid4

import pytest
from reliable_messaging.wire import Envelope, encode

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.messaging import (
    FIELDS,
    METADATA,
    SCHEMA,
    MessagingContractError,
)
from qs_ai.infrastructure.workflow_transport.transaction_command import FrozenCommandInput
from tests.route_fixtures import legacy_routes


def command_fixture(kind=pb.START):
    identity = str(uuid4())
    aggregate = identity if kind == pb.START else str(uuid4())
    value = pb.MessagingBody()
    command = getattr(value, FIELDS[kind])
    if kind in (pb.START, pb.CHANGE):
        command.actor.CopyFrom(workflow.Actor(org_id="7", subject_id="9"))
    else:
        command.scope.organization_id, command.scope.operator_user_id = 7, 9
    if kind == pb.START:
        command.request_id, command.testee_id = identity, "19"
        command.assessment_ids.append("21")
        command.goal = "first immutable input"
    elif kind in (pb.CHANGE, pb.PARTICIPANT_RETRY):
        command.command_id, command.session_id = identity, str(uuid4())
    else:
        command.scope.run_id = aggregate
        if kind == pb.EVALUATION_CANCEL:
            command.discard = False
    body = value.SerializeToString(deterministic=True)
    envelope = pb.MessagingEnvelope(
        schema_version=SCHEMA,
        producer="qs-server",
        destination="qs-ai",
        message_id=identity,
        kind=kind,
        aggregate_key=aggregate,
        original_occurred_at="2026-10-21T06:00:00+08:00",
        body_sha256=hashlib.sha256(body).hexdigest(),
        body_length=len(body),
        inline_body=body,
    )
    wire = encode(Envelope(identity, b"opaque-first-command-unit-wire", dict(METADATA)))
    return envelope, body, wire


@pytest.mark.parametrize(
    "kind", [pb.START, pb.CHANGE, pb.PARTICIPANT_RETRY, pb.EVALUATION_START, pb.EVALUATION_CANCEL]
)
def test_all_original_command_families_bind_the_same_canonical_owner_scope(kind):
    envelope, body, wire = command_fixture(kind)
    frozen = FrozenCommandInput.from_authenticated(envelope, body, wire, routes=legacy_routes())
    assert frozen.organization_id == "7"
    assert frozen.request.owner == "ai.mysql" and frozen.request.scope == "org:7"
    assert frozen.body_message().WhichOneof("value") == FIELDS[kind]


def test_original_command_owns_bytes_and_has_no_generated_receipt_input():
    envelope, body, wire = command_fixture()
    mutable = bytearray(body)
    frozen = FrozenCommandInput.from_authenticated(envelope, mutable, wire, routes=legacy_routes())
    request = frozen.request
    assert (request.owner, request.scope, request.key) == (
        "ai.mysql",
        "org:7",
        "ai-command/qs-server/" + envelope.message_id,
    )
    envelope.aggregate_key = "caller mutation"
    mutable[0] ^= 1
    frozen.envelope().aggregate_key = "detached copy mutation"
    frozen.body_message().start.goal = "detached body mutation"
    assert frozen.body == body and frozen.wire == wire and frozen.request == request
    assert frozen.body_message().start.goal == "first immutable input"
    assert repr(frozen) == "FrozenCommandInput()", "debug output must not include body/wire"


def test_changed_original_wire_keeps_business_key_but_conflicts_in_input_hash():
    envelope, body, wire = command_fixture()
    first = FrozenCommandInput.from_authenticated(envelope, body, wire, routes=legacy_routes())
    other = encode(Envelope(envelope.message_id, b"different protected wire", dict(METADATA)))
    changed = FrozenCommandInput.from_authenticated(envelope, body, other, routes=legacy_routes())
    assert first.request.key_digest == changed.request.key_digest
    assert first.request.input_hash != changed.request.input_hash


@pytest.mark.parametrize(
    "bad", ["body", "producer", "organization", "reference-org", "wire-id", "metadata"]
)
def test_invalid_command_input_cannot_be_frozen(bad):
    envelope, body, wire = command_fixture()
    if bad == "body":
        body += b"changed"
    elif bad == "producer":
        envelope.producer = "qs-ai"
    elif bad == "organization":
        value = pb.MessagingBody.FromString(body)
        value.start.actor.org_id = "007"
        body = value.SerializeToString(deterministic=True)
        envelope.inline_body = body
        envelope.body_length, envelope.body_sha256 = len(body), hashlib.sha256(body).hexdigest()
    elif bad == "reference-org":
        envelope.payload_reference.CopyFrom(
            pb.MessagePayloadReference(
                producer="qs-server",
                destination="qs-ai",
                message_id=envelope.message_id,
                organization_id="8",
                body_sha256=envelope.body_sha256,
                body_length=len(body),
            )
        )
    elif bad == "wire-id":
        wire = encode(Envelope(str(uuid4()), b"opaque", dict(METADATA)))
    else:
        wire = encode(Envelope(envelope.message_id, b"opaque", {}))
    with pytest.raises(MessagingContractError):
        FrozenCommandInput.from_authenticated(envelope, body, wire, routes=legacy_routes())


@pytest.mark.parametrize("field", ["header", "body", "wire"])
def test_original_command_refuses_empty_persisted_input(field):
    envelope, body, wire = command_fixture()
    frozen = FrozenCommandInput.from_authenticated(envelope, body, wire, routes=legacy_routes())
    with pytest.raises(MessagingContractError):
        replace(frozen, **{field: b""})
    with pytest.raises(MessagingContractError, match="original command bytes required"):
        replace(frozen, **{field: 17})  # never interpret an integer as a buffer allocation
