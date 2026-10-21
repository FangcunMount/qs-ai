"""Standalone first state wire/context is not a command, commit proof or model authorization."""

import hashlib
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.transaction import decode_plan, encode_plan

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport import frozen_start_events
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    FrozenInterpretationStateEvents,
    InterpretationStateFacts,
    InterpretationStateIntent,
    freeze_interpretation_state_plan,
    read_interpretation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from tests.test_evaluation_state_plan import fixture as state_fixture


@pytest.fixture
def frozen():
    f = state_fixture()
    intent = InterpretationStateIntent(
        "7",
        str(uuid4()),
        str(uuid4()),
        "finish/3",
        3,
        hashlib.sha256(b"original host business input").digest(),
    )
    facts = InterpretationStateFacts(intent, f.facts.at, b"first frozen business context")
    state = workflow.StateEvent(
        event_id=str(uuid4()),
        request_id=intent.request_id,
        session_id=intent.session_id,
        actor=workflow.Actor(org_id="7", subject_id="42"),
        testee_id="8",
        version=4,
        status="blocked",
        failure_code="model_not_connected",
    )

    def seal(value=state, clock=facts.at):
        return prepare(
            pb.INTERPRETATION_STATE,
            value.event_id,
            intent.request_id,
            pb.MessagingBody(interpretation_state=value),
            organization_id="7",
            original_occurred_at=clock.isoformat(timespec="microseconds"),
            **f.keys,
        )

    prepared = seal()
    plan = freeze_interpretation_state_plan(facts, str(uuid4()), prepared)
    return SimpleNamespace(**locals())


def test_first_wire_and_business_context_round_trip_without_keys_or_fake_command(
    frozen, monkeypatch
):
    p = frozen
    winner = decode_plan(encode_plan(p.plan))
    facts, archive = read_interpretation_state_plan(p.intent, winner)
    assert facts == p.facts and archive.wire == p.prepared.wire and archive.body == p.prepared.body
    assert archive.sequence == 4 and archive.requires_receipt and not archive.ordered
    assert InterpretationStateFacts.decode(facts.encode()) == facts

    def no_command(*args):
        raise AssertionError("Standalone state must not fabricate a command")

    monkeypatch.setattr(frozen_start_events, "read_admission_plan", no_command)
    events = FrozenInterpretationStateEvents(p.intent, winner)
    assert events.plan_digest == winner.digest
    assert not isinstance(events, frozen_start_events.FrozenInterpretationEvents)


def test_losing_cipher_clock_and_message_identity_do_not_change_stable_business_request(frozen):
    p = frozen
    facts = replace(p.facts, at=p.facts.at + timedelta(seconds=10))
    state = workflow.StateEvent.FromString(p.state.SerializeToString())
    state.event_id = str(uuid4())
    other = freeze_interpretation_state_plan(facts, str(uuid4()), p.seal(state, facts.at))
    p.intent.request.match(other)
    assert other.digest != p.plan.digest and other.branches[0].wire != p.plan.branches[0].wire
    assert read_interpretation_state_plan(p.intent, p.plan)[0].at == p.facts.at


@pytest.mark.parametrize("damage", ["owner", "scope", "normal_topic", "revision", "branch", "wire"])
def test_wrong_owner_physical_type_reference_or_wire_cannot_be_read_as_original_state(
    frozen, damage
):
    p = frozen
    with pytest.raises(ValueError):
        if damage == "owner":
            wrong = replace(p.plan, owner="qs.mysql")
        elif damage == "scope":
            wrong = replace(p.plan, scope="org:8")
        else:
            fields = {
                "normal_topic": {"physical_topic": "normal-topic"},
                "revision": {"route_revision": "m3-other"},
                "branch": {"branch_id": str(uuid4())},
                "wire": {"wire": b"wrong first wire"},
            }
            wrong = replace(p.plan, branches=(replace(p.plan.branches[0], **fields[damage]),))
        read_interpretation_state_plan(p.intent, wrong)


@pytest.mark.parametrize("field", ["session_id", "request_id", "version", "organization"])
def test_first_projection_cannot_cross_business_scope_or_version(frozen, field):
    p = frozen
    state = workflow.StateEvent.FromString(p.state.SerializeToString())
    if field == "organization":
        state.actor.org_id = "8"
    else:
        setattr(state, field, 5 if field == "version" else str(uuid4()))
    with pytest.raises(ValueError):
        freeze_interpretation_state_plan(p.facts, str(uuid4()), p.seal(state))


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "whitespace", "magic"])
def test_context_is_canonical_and_rejects_ambiguous_fields(frozen, damage):
    raw = frozen.facts.encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"expected_version":3}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"unrecognized":true}'
    elif damage == "whitespace":
        raw = raw.replace(b'"expected_version":3', b'"expected_version": 3')
    else:
        raw = raw.replace(b"/v1", b"/v2", 1)
    with pytest.raises(ValueError):
        InterpretationStateFacts.decode(raw)
