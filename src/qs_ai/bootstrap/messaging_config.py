"""Explicit host file readers, independent of DI and transport resource assembly."""

import json
from pathlib import Path
from typing import Any

from jwcrypto import jwk  # type: ignore[import-untyped]
from reliable_messaging.rocketmq import Config as RocketMQConfig
from reliable_messaging.transport import RouteBinding, validate_routes

from qs_ai.infrastructure.workflow_transport.messaging import ACKS, COMMANDS, EVENTS


def read_key(path: str, *, private: bool, expected_id: str | None = None) -> Any:
    # Never put path contents, JOSE errors, or private material in diagnostics.
    try:
        with Path(path).open("rb") as stream:
            data = stream.read(65537)
        if len(data) > 65536:
            raise ValueError
        key = jwk.JWK.from_json(data.decode())
        if not key.get("kid") or (expected_id is not None and key.get("kid") != expected_id):
            raise ValueError
        if private != key.has_private:
            raise ValueError
        if key.get("kty") != "EC" or key.get("crv") != "P-256":
            raise ValueError
        return key
    except Exception:
        raise ValueError("Invalid configured messaging key") from None


def read_rocketmq_config(options: Any) -> RocketMQConfig:
    """Read host-owned private files; SDK constructors never discover them."""
    config = options.rocketmq
    account = json.loads(Path(config.credentials_file).read_text())
    manifest = json.loads(Path(config.routes_file).read_text())
    version = manifest.get("version")
    if version not in (1, 2):
        raise ValueError("RocketMQ route manifest version1 or version2 required")
    allowed = {
        "topic",
        "subscription",
        "physical_topic",
        "consumer_group",
        "message_type",
        "owner",
        "route_revision",
    }
    all_routes = tuple(
        RouteBinding(**{k: v for k, v in r.items() if k in allowed}) for r in manifest["routes"]
    )
    if any(
        (version == 1 and (r.message_type or r.owner or r.route_revision))
        or (version == 2 and not r.message_type)
        for r in all_routes
    ):
        raise ValueError("RocketMQ route fields conflict with manifest version")
    validate_routes(all_routes)
    routes = tuple(r for r in all_routes if r.topic in {COMMANDS, ACKS, EVENTS})
    if {r.topic for r in routes} != {COMMANDS, ACKS, EVENTS}:
        raise ValueError("RocketMQ manifest missing required workflow topics")
    return RocketMQConfig(
        config.endpoint,
        account["username"],
        account["password"],
        routes,
        max_in_flight=options.max_in_flight,
    )
