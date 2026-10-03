"""Physical NSQ failure notifications never authorize business execution."""

import base64
import json
from collections.abc import Mapping
from urllib.parse import urlencode, urlsplit

from reliable_messaging.wire import FAILED_CHANNEL, FAILED_TYPE, Envelope, encode, failed_topic
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from qs_ai.infrastructure.workflow_transport.messaging import CHANNELS, valid_id


def failed_original(wire: bytes, topic: str) -> bytes:
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
            or value.get("type") != FAILED_TYPE
            or value.get("provider") != "nsq"
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


class FailureTopology:
    """One bounded read-only check of pre-provisioned failure Topic/Channel.

    The host supplies TCP-to-HTTP origins and a borrowed HTTP client. No topology
    creation, discovery loop, fallback endpoint or resource ownership is hidden.
    """

    def __init__(
        self,
        origins: Mapping[str, str],
        http: AsyncHTTPClient,
        topics: tuple[str, ...],
    ) -> None:
        if not origins or not topics or any(topic not in CHANNELS for topic in topics):
            raise ValueError("configured NSQD origins and subscriptions required")
        for address, origin in origins.items():
            url = urlsplit(origin)
            if (
                not address
                or url.scheme not in ("http", "https")
                or not url.hostname
                or url.username is not None
                or url.password is not None
                or url.path not in ("", "/")
                or url.query
                or url.fragment
            ):
                raise ValueError("explicit NSQD HTTP origin required")
        self.origins, self.http = dict(origins), http
        self.allowed = {failed_topic(topic, CHANNELS[topic]) for topic in topics}

    async def __call__(self, address: str, topic: str, channel: str) -> None:
        if address not in self.origins or topic not in self.allowed or channel != FAILED_CHANNEL:
            raise ValueError("failure subscription is not configured")
        url = (
            self.origins[address].rstrip("/")
            + "/stats?"
            + urlencode({"format": "json", "topic": topic, "channel": channel})
        )
        # A failure propagates to startup/REQ. No second endpoint or retry loop.
        response = await self.http.fetch(
            HTTPRequest(
                url,
                method="GET",
                connect_timeout=5,
                request_timeout=5,
                follow_redirects=False,
                headers={"Accept": "application/vnd.nsq; version=1.0"},
            )
        )
        if response.code != 200 or len(response.body) > 1024 * 1024:
            raise ValueError("failure subscription unavailable")
        try:
            document = json.loads(response.body)
            ready = any(
                t.get("topic_name") == topic
                and not t.get("paused", False)
                and any(
                    c.get("channel_name") == channel and not c.get("paused", False)
                    for c in t.get("channels", [])
                )
                for t in document["topics"]
            )
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ValueError("failure subscription unavailable") from None
        if not ready:
            raise ValueError("failure subscription must be pre-provisioned")
