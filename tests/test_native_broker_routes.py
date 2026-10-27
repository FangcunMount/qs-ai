"""Native fixture route selection is exact; no new path may silently use v2 resources."""

import json

import pytest
from reliable_messaging.transport import consumer_group_v3, physical_topic_v3

from qs_ai.infrastructure.workflow_transport.messaging import ACKS, COMMANDS, EVENTS
from tests.integration.broker_routes import (
    _routes,
    owned_broker_physical,
    owned_broker_route,
    owned_broker_routes,
)
from tests.route_fixtures import legacy_routes


@pytest.fixture
def native_routes(tmp_path, monkeypatch):
    path = tmp_path / "owned-config.json"
    rows = []
    for topic, owner in ((COMMANDS, "qs.mysql"), (ACKS, "qs.mysql"), (EVENTS, "ai.mysql")):
        rows.append(
            dict(
                topic=topic,
                owner=owner,
                subscription="native-observer",
                message_type="TRANSACTION",
                route_revision="m3-1",
                naming_scheme="readable-v1",
                physical_topic=physical_topic_v3(topic, "TRANSACTION", owner, "m3-1"),
                consumer_group=consumer_group_v3(topic, "native-observer"),
            )
        )
    path.write_text(json.dumps(dict(routes=rows)))
    path.chmod(0o600)
    monkeypatch.setenv("QS_AI_M3_BROKER_CONFIG", str(path))
    _routes.cache_clear()
    try:
        yield path, rows
    finally:
        _routes.cache_clear()


@pytest.mark.parametrize(
    "topic,owner", [(COMMANDS, "qs.mysql"), (ACKS, "qs.mysql"), (EVENTS, "ai.mysql")]
)
def test_exact_native_manifest_controls_all_senders_and_observers(native_routes, topic, owner):
    route = owned_broker_route(topic, "native-observer", owner)
    assert route.physical_topic == owned_broker_physical(topic, owner)
    assert route.consumer_group == consumer_group_v3(topic, "native-observer")
    assert route.naming_scheme == "readable-v1"
    with pytest.raises(ValueError):
        owned_broker_route(topic, "foreign-subscription", owner)
    with pytest.raises(ValueError):
        owned_broker_physical(topic, "foreign-owner")


def test_malformed_readable_binding_does_not_fall_back_to_legacy(native_routes):
    path, rows = native_routes
    rows[0]["physical_topic"] = "foreign"
    path.write_text(json.dumps(dict(routes=rows)))
    with pytest.raises(ValueError):
        owned_broker_routes()


def test_original_named_v2_fixture_keeps_explicit_inert_legacy_contract(native_routes):
    path, rows = native_routes
    from reliable_messaging.transport import RouteKey

    route = legacy_routes().publish_route(RouteKey(COMMANDS, "TRANSACTION", "qs.mysql", "m3-1"))
    row = dict(rows[0], naming_scheme="", physical_topic=route.physical_topic)
    from reliable_messaging.transport import consumer_group_v2

    row["consumer_group"] = consumer_group_v2(COMMANDS, row["subscription"])
    path.write_text(json.dumps(dict(routes=[row])))
    assert owned_broker_physical(COMMANDS, "qs.mysql") == route.physical_topic
    assert owned_broker_route(COMMANDS, "native-observer", "qs.mysql").naming_scheme == ""
