"""Trusted, immutable routes for an owned native fixture; never used by application code."""

import json
import os
from functools import lru_cache
from pathlib import Path

from reliable_messaging.transport import (
    RouteBinding,
    RouteKey,
    RouteResolver,
    consumer_group_v2,
    parse_route_manifest,
)

from tests.route_fixtures import legacy_routes


@lru_cache(maxsize=1)
def _routes(path: str) -> tuple[RouteResolver, bool]:
    config = json.loads(Path(path).read_text())
    rows = config["routes"]
    readable = any(row.get("naming_scheme") for row in rows)
    if not readable:
        # Older named native profiles deliberately own only a subset of the v2 fixture.
        # The untouched fixture supplies inert ACK validation; it grants no Broker permission.
        parse_route_manifest(json.dumps({"version": 2, "routes": rows}))
        return legacy_routes(), False
    parsed = parse_route_manifest(json.dumps({"version": 3, "routes": rows}))
    return RouteResolver(parsed.routes, parsed.historical_routes), True


def owned_broker_routes() -> RouteResolver:
    return _routes(os.environ["QS_AI_M3_BROKER_CONFIG"])[0]


def owned_broker_physical(topic: str, owner: str) -> str:
    return (
        owned_broker_routes()
        .publish_route(RouteKey(topic, "TRANSACTION", owner, "m3-1"))
        .physical_topic
    )


def owned_broker_route(topic: str, subscription: str, owner: str) -> RouteBinding:
    resolver, readable = _routes(os.environ["QS_AI_M3_BROKER_CONFIG"])
    key = RouteKey(topic, "TRANSACTION", owner, "m3-1")
    if not readable:
        route = resolver.publish_route(key)
        return RouteBinding(
            topic,
            subscription,
            route.physical_topic,
            consumer_group_v2(topic, subscription),
            "TRANSACTION",
            owner,
            "m3-1",
        )
    selected = tuple(
        route
        for route in resolver.subscription_routes(topic, subscription)
        if route.owner == owner and route.message_type == "TRANSACTION"
    )
    if len(selected) != 1:
        raise ValueError("One exact configured native subscription required")
    return selected[0]
