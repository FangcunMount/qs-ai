"""Stable operation identity with an explicit later authoritative source version."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_state_plan import fixture


def later():
    f = fixture()
    facts = replace(
        f.facts,
        previous_sequence=9,
        previous_version=3,
        source_version=3,
        business_context=b"frozen original finalization",
    )
    state = pb.EvaluationRuntimeState.FromString(f.state.SerializeToString())
    state.version, state.event_sequence = 4, 10
    first = f.seal(value=state, at=facts.at)
    plan = freeze_evaluation_state_plan(facts, str(uuid4()), first, route_resolver=legacy_routes())
    return f, facts, state, first, plan


def test_existing_v1_and_v2_context_bytes_remain_exact():
    intent = EvaluationStateIntent(
        "7",
        "00000000-0000-0000-0000-000000000001",
        "preflight/2",
        2,
        hashlib.sha256(b"original source").digest(),
    )
    facts = EvaluationStateFacts(intent, datetime(2026, 10, 21, tzinfo=UTC), 8, 2)
    assert (
        hashlib.sha256(facts.encode()).hexdigest()
        == "f28022c500e38d60dba25a8996ec5583a18e8a5eee3e24780748abcd66a85585"
    )
    assert (
        hashlib.sha256(
            replace(facts, business_context=b"first deterministic completion").encode()
        ).hexdigest()
        == "ecb9989a8db5f5bc50e20e4c0ef63dececf7eaf609f05c2f6d1c3ef021233f8f"
    )


def test_later_source_owns_frozen_final_version_and_exact_sequence_without_changing_request():
    f, facts, state, first, plan = later()
    assert facts.encode().startswith(b"qs-ai-evaluation-state/v3\0")
    restored, archived = read_evaluation_state_plan(
        f.intent, decode_plan(encode_plan(plan)), route_resolver=legacy_routes()
    )
    assert restored == facts and restored.source_version == 3 and restored.sequence == 10
    assert facts.intent.request == f.facts.intent.request
    assert archived.wire == first.wire and archived.body == first.body
    assert pb.MessagingBody.FromString(archived.body).evaluation_state == state
    with pytest.raises(ValueError):
        replace(f.facts, previous_version=3)  # Old context does not gain relaxed bounds.


def test_changed_authoritative_source_is_a_different_plan_for_the_same_business_request():
    f, facts, state, first, old = later()
    changed = replace(facts, source_version=4, previous_sequence=10, previous_version=4)
    state.version, state.event_sequence = 5, 11
    new = freeze_evaluation_state_plan(
        changed, str(uuid4()), f.seal(value=state, at=changed.at), route_resolver=legacy_routes()
    )
    assert old.digest != new.digest and old.branches[0].wire == first.wire
    f.intent.request.match(old)
    f.intent.request.match(new)
    assert changed.sequence == 11


@pytest.mark.parametrize("source", [True, False, -1, 1, 2**63 - 1, 2**63, "3", 3.0])
def test_source_version_is_bounded_monotonic_integer(source):
    f = fixture()
    with pytest.raises(ValueError):
        replace(f.facts, source_version=source)


def test_previous_version_and_wire_target_must_match_explicit_source():
    f, facts, state, first, plan = later()
    with pytest.raises(ValueError):
        replace(facts, previous_version=4)
    with pytest.raises(ValueError):
        freeze_evaluation_state_plan(
            replace(facts, source_version=4), str(uuid4()), first, route_resolver=legacy_routes()
        )
    state.version = 3
    with pytest.raises(ValueError):
        freeze_evaluation_state_plan(
            facts, str(uuid4()), f.seal(value=state, at=facts.at), route_resolver=legacy_routes()
        )


@pytest.mark.parametrize(
    "damage", ["missing", "extra", "boolean", "duplicate", "old_header", "missing_context", "null"]
)
def test_new_context_rejects_ambiguous_or_cross_version_fields(damage):
    _, facts, *_ = later()
    magic = b"qs-ai-evaluation-state/v3\0"
    value = json.loads(facts.encode()[len(magic) :])
    if damage == "missing":
        value.pop("source_version")
    elif damage == "extra":
        value["unbound"] = True
    elif damage == "boolean":
        value["source_version"] = True
    elif damage == "missing_context":
        value.pop("business_context")
    elif damage == "null":
        value["source_version"] = None
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"source_version":3}'
    if damage == "old_header":
        magic = b"qs-ai-evaluation-state/v2\0"
    with pytest.raises(ValueError):
        EvaluationStateFacts.decode(magic + raw)


def test_explicit_source_stops_before_signed_target_version_overflow():
    f = fixture()
    facts = replace(f.facts, source_version=2**63 - 2, previous_version=2**63 - 2)
    state = pb.EvaluationRuntimeState.FromString(f.state.SerializeToString())
    state.version, state.event_sequence = 2**63 - 1, facts.sequence
    plan = freeze_evaluation_state_plan(
        facts, str(uuid4()), f.seal(value=state, at=facts.at), route_resolver=legacy_routes()
    )
    assert read_evaluation_state_plan(f.intent, plan, route_resolver=legacy_routes())[0] == facts


def test_source_binding_allows_frozen_multiple_original_checkpoint_advances_in_one_root():
    f, facts, state, first, plan = later()
    state.version = 5  # source3 -> original prepare4 -> original final5, one state event
    multi = freeze_evaluation_state_plan(
        facts, str(uuid4()), f.seal(value=state, at=facts.at), route_resolver=legacy_routes()
    )
    restored, archived = read_evaluation_state_plan(f.intent, multi, route_resolver=legacy_routes())
    assert restored.source_version == 3 and archived.sequence == 10
    assert pb.MessagingBody.FromString(archived.body).evaluation_state.version == 5
    f.intent.request.match(multi)


def test_explicit_source_with_empty_business_context_is_canonical_and_request_stable():
    f = fixture()
    facts = replace(f.facts, source_version=2)
    assert EvaluationStateFacts.decode(facts.encode()) == facts
    assert facts.encode().startswith(b"qs-ai-evaluation-state/v3\0")
    plan = freeze_evaluation_state_plan(
        facts, str(uuid4()), f.prepared, route_resolver=legacy_routes()
    )
    f.intent.request.match(plan)
    assert (
        read_evaluation_state_plan(f.intent, plan, route_resolver=legacy_routes())[1].wire
        == f.prepared.wire
    )
