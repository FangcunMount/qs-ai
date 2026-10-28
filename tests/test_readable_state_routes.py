"""Readable active routes preserve original state bytes; history is exact and passive."""

from dataclasses import replace

import pytest
from reliable_messaging.transaction import decode_plan, encode_plan
from reliable_messaging.transport import RouteKey

from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    freeze_interpretation_state_plan,
    read_interpretation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS
from tests.route_fixtures import readable_routes
from tests.test_evaluation_state_plan import fixture as evaluation_fixture
from tests.test_interpretation_state_plan import frozen as interpretation_frozen  # noqa: F401


def prove_routes(frozen, freeze, read):
    routes = readable_routes()
    old_bytes = encode_plan(frozen.plan)
    old_facts, old_archive = read(frozen.intent, frozen.plan, route_resolver=routes)
    current = freeze(frozen.facts, frozen.plan.operation_id, frozen.prepared, route_resolver=routes)
    assert current.digest != frozen.plan.digest
    assert current.operation_id == frozen.plan.operation_id
    assert current.input_hash == frozen.plan.input_hash
    assert current.branches[0].wire == frozen.plan.branches[0].wire == old_archive.wire
    assert current.branches[0].message == frozen.plan.branches[0].message
    key = RouteKey(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    assert current.branches[0].physical_topic == routes.publish_route(key).physical_topic
    assert current.branches[0].physical_topic == "qs-ai-events-v1--tx--ai-mysql--m3-1"
    new_facts, new_archive = read(frozen.intent, current, route_resolver=routes)
    assert old_facts == new_facts and old_archive == new_archive
    assert encode_plan(frozen.plan) == old_bytes
    assert decode_plan(old_bytes) == frozen.plan
    assert routes.recorded_route(key, frozen.plan.branches[0].physical_topic)
    with pytest.raises(ValueError):
        read(frozen.intent, frozen.plan, route_resolver=readable_routes(history=False))
    with pytest.raises(ValueError):
        read(
            frozen.intent,
            replace(current, branches=(replace(current.branches[0], physical_topic="foreign"),)),
            route_resolver=routes,
        )


def test_evaluation_state_first_wire_and_exact_passive_history():
    prove_routes(evaluation_fixture(), freeze_evaluation_state_plan, read_evaluation_state_plan)


def test_interpretation_state_first_wire_and_exact_passive_history(interpretation_frozen):  # noqa: F811
    prove_routes(
        interpretation_frozen, freeze_interpretation_state_plan, read_interpretation_state_plan
    )
