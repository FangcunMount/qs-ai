import base64
import hashlib
import json
import os
import subprocess
from dataclasses import replace

import pytest
from jwcrypto import jwk
from reliable_messaging.protected import ProtectionError, TrustedSigner

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    MessagingContractError,
    authenticate,
    parse_body,
    prepare,
)

ID = "b1941896-e7df-4b9d-9417-b80bd05276b9"
EVENT_ID = "279e6c52-5519-49ec-80a0-4b63122de9cb"
BODY_HASH = "a" * 64


@pytest.fixture
def keys():
    return (
        jwk.JWK.generate(kty="EC", crv="P-256", kid="sender.sign.1"),
        jwk.JWK.generate(kty="EC", crv="P-256", kid="recipient.encrypt.1"),
    )


def message(kind, keys, *, large=False):
    body = pb.MessagingBody()
    identity = ID
    if kind == pb.START:
        body.start.CopyFrom(
            workflow.StartCommand(
                request_id=ID,
                actor=workflow.Actor(org_id="18446744073709551615", subject_id="中文🙂"),
                goal="<>&\u2028",
                testee_id="18446744073709551615",
            )
        )
    elif kind == pb.CHANGE:
        body.change.CopyFrom(
            workflow.ChangeCommand(command_id=ID, answer="", expected_version=2**63 - 1)
        )
    elif kind == pb.PARTICIPANT_RETRY:
        body.participant_retry.command_id = ID
    elif kind == pb.EVALUATION_START:
        body.evaluation_start.scope.run_id = ID
    elif kind == pb.EVALUATION_CANCEL:
        body.evaluation_cancel.discard = False
    elif kind == pb.COMMAND_RECEIPT:
        identity = EVENT_ID
        body.command_receipt.CopyFrom(
            pb.MessagingCommandReceipt(
                command_id=ID,
                command_body_sha256=BODY_HASH,
                decision=pb.REJECTED,
                code="capacity_full",
            )
        )
    elif kind == pb.INTERPRETATION_STATE:
        identity = EVENT_ID
        body.interpretation_state.CopyFrom(
            workflow.StateEvent(
                event_id=EVENT_ID,
                request_id=ID,
                artifact_json="🙂" * (32768 if large else 1),
            )
        )
    elif kind == pb.EVALUATION_STATE:
        identity = EVENT_ID
        body.evaluation_state.CopyFrom(
            pb.EvaluationRuntimeState(
                run_id=ID,
                organization_id="18446744073709551615",
                event_sequence=2**64 - 1,
            )
        )
    else:
        body.event_acknowledgement.CopyFrom(
            pb.MessagingEventAcknowledgement(
                event_id=EVENT_ID,
                event_body_sha256=BODY_HASH,
                event_kind=pb.COMMAND_RECEIPT,
                outcome=pb.MessagingEventAcknowledgement.STORED,
            )
        )
    return prepare(
        kind,
        identity,
        "original:" + ID,
        body,
        organization_id="18446744073709551615",
        signing_key=keys[0],
        recipient_key=keys[1],
        correlation=ID,
        original_occurred_at="2026-10-03T12:34:56.123456+08:00",
    )


def open_prepared(prepared, keys, topic=None):
    return authenticate(
        prepared.wire,
        topic or prepared.topic,
        decrypt_keys={keys[1].get("kid"): keys[1]},
        trusted_signers={keys[0].get("kid"): TrustedSigner(prepared.envelope.producer, keys[0])},
    )


@pytest.mark.parametrize("kind", range(pb.START, pb.EVENT_ACKNOWLEDGEMENT + 1))
def test_all_message_families_roundtrip_original_identity(keys, kind):
    prepared = message(kind, keys)
    envelope = open_prepared(prepared, keys)
    assert envelope == prepared.envelope
    body = parse_body(envelope, prepared.body)
    assert envelope.body_sha256 == hashlib.sha256(prepared.body).hexdigest()
    assert envelope.original_occurred_at == "2026-10-03T12:34:56.123456+08:00"
    if kind == pb.CHANGE:
        assert body.change.HasField("answer") and body.change.answer == ""
        assert body.change.expected_version == 2**63 - 1
    if kind == pb.EVALUATION_CANCEL:
        assert body.evaluation_cancel.HasField("discard") and not body.evaluation_cancel.discard
    if kind == pb.EVALUATION_STATE:
        assert body.evaluation_state.event_sequence == 2**64 - 1


def test_original_large_artifact_uses_reference_without_resealing(keys):
    prepared = message(pb.INTERPRETATION_STATE, keys, large=True)
    assert len(prepared.body) > 128 * 1024
    assert len(prepared.wire) < 8192
    assert prepared.envelope.WhichOneof("body") == "payload_reference"
    envelope = open_prepared(prepared, keys)
    assert parse_body(envelope, prepared.body).interpretation_state.artifact_json == "🙂" * 32768
    with pytest.raises(MessagingContractError):
        parse_body(envelope, prepared.body[:-1])
    envelope.payload_reference.organization_id = "01"
    with pytest.raises(MessagingContractError):
        parse_body(envelope, prepared.body)


def test_wrong_topic_key_and_kind_are_rejected(keys):
    prepared = message(pb.START, keys)
    with pytest.raises(ProtectionError):
        open_prepared(prepared, keys, EVENTS)
    envelope = open_prepared(prepared, keys)
    envelope.kind = pb.CHANGE
    with pytest.raises(MessagingContractError):
        parse_body(envelope, prepared.body)
    other = jwk.JWK.generate(kty="EC", crv="P-256", kid="sender.sign.1")
    with pytest.raises(ProtectionError):
        open_prepared(prepared, (other, keys[1]))


@pytest.mark.interop
@pytest.mark.parametrize("kind", range(pb.START, pb.EVENT_ACKNOWLEDGEMENT + 1))
def test_go_python_messaging_interop(keys, kind):
    binary = os.environ.get("QS_MQ_CONTRACT_PROBE")
    if not binary:
        pytest.fail("Required fixed QS_MQ_CONTRACT_PROBE is missing")
    prepared = message(kind, keys, large=kind == pb.INTERPRETATION_STATE)
    header = prepared.envelope
    request = {
        "Kind": kind,
        "ID": header.message_id,
        "Aggregate": header.aggregate_key,
        "Correlation": header.correlation_command_id,
        "Organization": "18446744073709551615",
        "OriginalTime": header.original_occurred_at,
        "Topic": prepared.topic,
        "Body": base64.b64encode(prepared.body).decode(),
        "Signing": json.loads(keys[0].export(private_key=True)),
        "Encryption": json.loads(keys[1].export(private_key=True)),
    }
    result = subprocess.run(
        [binary],
        input=json.dumps(
            {**request, "Mode": "authenticate", "Wire": base64.b64encode(prepared.wire).decode()}
        ).encode(),
        capture_output=True,
        check=True,
    )
    # Protobuf deterministic encoders are not cross-language canonical encoders.
    # Verify retained semantic fields and the original byte hash, never rehash a re-encoding.
    assert pb.MessagingEnvelope.FromString(base64.b64decode(result.stdout)) == header
    result = subprocess.run(
        [binary],
        input=json.dumps({**request, "Mode": "protect"}).encode(),
        capture_output=True,
        check=True,
    )
    result_document = json.loads(result.stdout)
    from_go = open_prepared(replace(prepared, wire=base64.b64decode(result_document["Wire"])), keys)
    go_body = base64.b64decode(result_document["Body"])
    assert from_go.message_id == header.message_id
    assert from_go.original_occurred_at == header.original_occurred_at
    assert parse_body(from_go, go_body) == parse_body(header, prepared.body)
