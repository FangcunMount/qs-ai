"""Explicit host file readers, independent of DI and transport resource assembly."""

import json
from pathlib import Path
from typing import Any

from jwcrypto import jwk  # type: ignore[import-untyped]
from reliable_messaging.rocketmq import Config as RocketMQConfig
from reliable_messaging.transport import parse_route_manifest

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
    manifest = parse_route_manifest(Path(config.routes_file).read_bytes())
    routes = tuple(r for r in manifest.routes if r.topic in {COMMANDS, ACKS, EVENTS})
    history = tuple(r for r in manifest.historical_routes if r.topic in {COMMANDS, ACKS, EVENTS})
    if {r.topic for r in routes} != {COMMANDS, ACKS, EVENTS}:
        raise ValueError("RocketMQ manifest missing required workflow topics")
    if options.transactions:
        # File preflight, sender assembly and subscriber assembly must agree
        # before a new deployment can replace the running service.
        for topic, owner in ((COMMANDS, "qs.mysql"), (ACKS, "qs.mysql"), (EVENTS, "ai.mysql")):
            selected = [r for r in routes if r.topic == topic]
            if not selected or any(
                (r.message_type, r.owner, r.route_revision) != ("TRANSACTION", owner, "m3-1")
                for r in selected
            ):
                raise ValueError("Transaction workflow route binding differs")
    return RocketMQConfig(
        config.endpoint,
        account["username"],
        account["password"],
        routes,
        max_in_flight=options.max_in_flight,
        historical_routes=history,
    )
