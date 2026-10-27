"""Host manifest parsing is inert and keeps historical routes out of admission."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from reliable_messaging.transport import (
    RouteBinding,
    consumer_group_v2,
    consumer_group_v3,
    physical_topic_v2,
    physical_topic_v3,
)

from qs_ai.bootstrap.messaging_config import read_rocketmq_config
from qs_ai.infrastructure.workflow_transport.messaging import ACKS, COMMANDS, EVENTS


def readable_options(tmp_path, *, readable=True, history=True):
    active, previous = [], []
    for topic, owner, sub in (
        (COMMANDS, "qs.mysql", COMMANDS),
        (ACKS, "qs.mysql", ACKS),
        (EVENTS, "ai.mysql", "qs-server.ai-events.v1"),
    ):
        old = RouteBinding(
            topic=topic,
            physical_topic=physical_topic_v2(topic, "TRANSACTION", owner, "m3-1"),
            subscription=sub,
            consumer_group=consumer_group_v2(topic, sub),
            message_type="TRANSACTION",
            owner=owner,
            route_revision="m3-1",
        )
        previous.append(old)
        active.append(
            replace(
                old,
                physical_topic=physical_topic_v3(topic, "TRANSACTION", owner, "m3-1"),
                consumer_group=consumer_group_v3(topic, sub),
                naming_scheme="readable-v1",
            )
            if readable
            else old
        )
    credentials = tmp_path / "account.json"
    credentials.write_text(json.dumps({"username": "fixture", "password": "disposable"}))
    manifest = tmp_path / "routes.json"
    value = {"version": 3 if readable else 2, "routes": [asdict(r) for r in active]}
    if readable and history:
        value["historical_routes"] = [asdict(r) for r in previous]
    manifest.write_text(json.dumps(value))
    return SimpleNamespace(
        rocketmq=SimpleNamespace(
            credentials_file=str(credentials), routes_file=str(manifest), endpoint="127.0.0.1:8081"
        ),
        max_in_flight=4,
        transactions={"ai.mysql": object()},
    )


def test_v3_host_keeps_history_passive_and_preserves_typed_owner(tmp_path):
    options = readable_options(tmp_path)
    config = read_rocketmq_config(options)
    assert len(config.routes) == len(config.historical_routes) == 3
    assert {r.naming_scheme for r in config.routes} == {"readable-v1"}
    assert all(r.physical_topic.startswith("rmq2_t_") for r in config.historical_routes)
    assert {r.physical_topic for r in config.routes}.isdisjoint(
        r.physical_topic for r in config.historical_routes
    )
    assert config.max_in_flight == 4


def test_v2_host_mapping_remains_unchanged(tmp_path):
    config = read_rocketmq_config(readable_options(tmp_path, readable=False))
    assert len(config.routes) == 3
    assert config.historical_routes == ()
    assert all(r.physical_topic.startswith("rmq2_t_") for r in config.routes)


def test_v3_host_cannot_infer_missing_scheme(tmp_path):
    options = readable_options(tmp_path)
    from pathlib import Path

    path = Path(options.rocketmq.routes_file)
    value = json.loads(path.read_text())
    del value["routes"][0]["naming_scheme"]
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        read_rocketmq_config(options)


def test_v3_sender_registers_only_active_owner_topics_and_keeps_history_passive(
    tmp_path, monkeypatch
):
    from reliable_messaging.transport import RouteKey
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from qs_ai.bootstrap.transaction_messaging import make_transaction_sender
    from qs_ai.config import MessagingOptions
    from tests.test_transaction_messaging import options

    source = options(tmp_path)
    named = readable_options(tmp_path)
    source["rocketmq"]["routes_file"] = named.rocketmq.routes_file
    source["rocketmq"]["credentials_file"] = named.rocketmq.credentials_file
    value = MessagingOptions.model_validate(source)
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", value.rocketmq.ca_file)

    class NeverSession(AsyncSession):
        def __init__(self, *args, **kwargs):
            raise AssertionError("constructor opened an original business session")

    runtime = make_transaction_sender(value, async_sessionmaker(class_=NeverSession))
    assert len(runtime.config.routes) == len(runtime.config.historical_routes) == 1
    route = runtime.config.routes[0]
    assert route.logical_topic == EVENTS and route.naming_scheme == "readable-v1"
    assert route.physical_topic == "qs-ai-events-v1--tx--ai-mysql--m3-1"
    assert runtime.config.historical_routes[0].physical_topic.startswith("rmq2_t_")
    assert runtime.config.historical_routes[0].naming_scheme == ""
    assert (
        runtime.routing.publish_route(
            RouteKey(ACKS, "TRANSACTION", "qs.mysql", "m3-1")
        ).physical_topic
        == "qs-ai-acks-v1--tx--qs-mysql--m3-1"
    )
    assert not runtime.stats()["ready"]


def test_admission_first_wire_and_input_identity_survive_exact_historical_mapping(tmp_path):
    from reliable_messaging.transport import RouteResolver

    from qs_ai.infrastructure.workflow_transport.transaction_command import (
        freeze_admission_plan,
        read_admission_plan,
    )
    from tests.test_admission_plan import fixture

    incoming, receipt, state = fixture()
    old = freeze_admission_plan(
        incoming, "353a53c4-9ede-4701-88c5-7e626b8cec30", ((receipt, 2), (state, 2))
    )
    config = read_rocketmq_config(readable_options(tmp_path))
    routes = RouteResolver(config.routes, config.historical_routes)
    current_input = replace(incoming, routes=routes)
    assert current_input.request == incoming.request
    assert current_input == incoming  # route capability is not business input
    current = freeze_admission_plan(current_input, old.operation_id, ((receipt, 2), (state, 2)))
    assert all(b.physical_topic == "qs-ai-events-v1--tx--ai-mysql--m3-1" for b in current.branches)
    assert [b.wire for b in current.branches] == [b.wire for b in old.branches]
    assert current.digest != old.digest
    assert [a.wire for a in read_admission_plan(current_input, old)] == [receipt.wire, state.wire]
    assert [a.wire for a in read_admission_plan(current_input, current)] == [
        receipt.wire,
        state.wire,
    ]
    active_only = replace(current_input, routes=RouteResolver(config.routes))
    with pytest.raises(ValueError):
        read_admission_plan(active_only, old)
