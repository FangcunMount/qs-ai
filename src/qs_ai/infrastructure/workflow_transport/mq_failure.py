"""Physical NSQ failure notifications never authorize business execution."""

import base64
import json

from reliable_messaging.wire import Envelope, encode

from qs_ai.infrastructure.workflow_transport.messaging import CHANNELS, valid_id


def failed_original(wire: bytes, topic: str, *, rocketmq_topic: str) -> bytes:
    """Recover only the signed original transport, ignoring claimed failure counts.

    Failure wrappers are not signed. A matching wrapper is still untrusted until
    the receiver authenticates the reconstructed original with local JOSE keys.
    """
    if len(wire) > 262_144 or topic not in CHANNELS:
        raise ValueError("invalid failure notification")
    try:
        value = json.loads(wire)
        if (
            not isinstance(value, dict)
            or (value.get("type"), value.get("provider"))
            != ("reliable-messaging.failed.v1", "rocketmq")
            or not rocketmq_topic
            or value.get("physical_topic") != rocketmq_topic
            or value.get("topic") != topic
            or value.get("channel") != CHANNELS[topic]
            or not isinstance(value.get("uuid"), str)
            or not valid_id(value["uuid"])
            or not isinstance(value.get("payload"), str)
        ):
            raise ValueError("invalid failure notification")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, dict) or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in metadata.items()
        ):
            raise ValueError("invalid failure notification")
        original = encode(
            Envelope(value["uuid"], base64.b64decode(value["payload"], validate=True), metadata)
        )
        if len(original) > 262_144:
            raise ValueError("invalid failure notification")
        return original
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise ValueError("invalid failure notification") from None
