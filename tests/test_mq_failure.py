"""Failure wrappers remain untrusted until the caller authenticates original wire."""

import base64
import json
from uuid import uuid4

import pytest
from reliable_messaging.transport import physical_topic_v2
from reliable_messaging.wire import Envelope, encode

from qs_ai.infrastructure.workflow_transport.messaging import CHANNELS, COMMANDS
from qs_ai.infrastructure.workflow_transport.mq_failure import failed_original

TOPIC = physical_topic_v2(COMMANDS, "TRANSACTION", "qs.mysql", "m3-1")


def notice():
    envelope = Envelope(str(uuid4()), b"opaque signed payload", {"secure_profile": "fixture"})
    value = dict(
        type="reliable-messaging.failed.v1",
        provider="rocketmq",
        physical_topic=TOPIC,
        topic=COMMANDS,
        channel=CHANNELS[COMMANDS],
        uuid=envelope.message_id,
        payload=base64.b64encode(envelope.payload).decode(),
        metadata=envelope.metadata,
        attempts=65535,
    )
    return envelope, value


def test_current_notice_reconstructs_original_wire_without_trusting_attempts():
    envelope, value = notice()
    assert failed_original(json.dumps(value).encode(), COMMANDS, rocketmq_topic=TOPIC) == encode(
        envelope
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider", "nsq"),
        ("type", "old.failed.v1"),
        ("physical_topic", "other"),
        ("topic", "other"),
        ("channel", "other"),
        ("uuid", "wrong"),
        ("payload", "??"),
        ("metadata", {"private": 1}),
    ],
)
def test_old_or_wrong_transport_reference_cannot_reconstruct_current_message(field, value):
    _, data = notice()
    data[field] = value
    with pytest.raises(ValueError):
        failed_original(json.dumps(data).encode(), COMMANDS, rocketmq_topic=TOPIC)


@pytest.mark.parametrize("wire", [b"[]", b"invalid", b"x" * 262145])
def test_invalid_or_oversized_notice_is_rejected(wire):
    with pytest.raises(ValueError):
        failed_original(wire, COMMANDS, rocketmq_topic=TOPIC)
