"""Frozen local START facts are read from the durable winner, never regenerated."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from qs_ai.application.interpretation.service import fingerprint
from qs_ai.infrastructure.persistence.mysql.execution_configurations import PlannedConfiguration
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    freeze_admission_plan,
    read_admission_plan,
)
from tests.test_admission_plan import fixture


def facts_fixture():
    incoming, receipt, state = fixture()
    from qs_ai.contracts.workflow import messaging_pb2 as pb

    result = pb.MessagingBody.FromString(receipt.body).command_receipt.workflow_receipt
    evidence_id = str(uuid4())
    facts = StartPlanFacts(
        incoming.request.input_hash,
        datetime(2026, 10, 21, 6, 30, tzinfo=UTC),
        result.session_id,
        result.run_id,
        evidence_id,
        str(uuid4()),
        str(uuid4()),
        state.envelope.message_id,
        PlannedConfiguration(
            result.session_id,
            evidence_id,
            fingerprint([]),
            str(uuid4()),
            "b" * 64,
            3,
            '{"organization_id":7}',
        ),
    )
    plan = freeze_admission_plan(
        incoming, str(uuid4()), ((receipt, 2), (state, 2)), context=facts.encode()
    )
    return incoming, facts, plan


def test_start_facts_round_trip_freezes_clock_identity_and_exact_configuration():
    incoming, facts, plan = facts_fixture()
    restored = StartPlanFacts.from_plan(incoming, plan)
    assert restored == facts
    assert restored.identity("session") == facts.session_id
    assert restored.identity("run") == facts.run_id
    assert restored.identity("evidence") == facts.evidence_id
    assert restored.at == facts.at
    assert all(v.context == facts.encode() for v in read_admission_plan(incoming, plan))


@pytest.mark.parametrize(
    "field",
    ["session_id", "run_id", "evidence_id", "job_id", "reservation_token", "state_event_id"],
)
def test_start_facts_refuses_invalid_frozen_identity(field):
    _, facts, _ = facts_fixture()
    with pytest.raises(ValueError):
        replace(facts, **{field: "invalid"})


@pytest.mark.parametrize("mutation", ["input", "session", "run", "state"])
def test_context_must_match_exact_first_business_receipt_and_state(mutation):
    incoming, facts, plan = facts_fixture()
    if mutation == "input":
        changed = replace(facts, input_hash=b"z" * 32)
    elif mutation in ("session", "run"):
        changed = replace(facts, configuration=None, **{mutation + "_id": str(uuid4())})
    else:
        changed = replace(facts, state_event_id=str(uuid4()))
    from qs_ai.infrastructure.workflow_transport.messaging import PreparedMessage
    from qs_ai.infrastructure.workflow_transport.transaction_archive import decode_branch

    publications = tuple(
        (PreparedMessage(v.envelope(), v.body, v.wire, branch.message.destination), v.sequence)
        for branch in plan.branches
        for v in (decode_branch(branch),)
    )
    wrong = freeze_admission_plan(
        incoming, plan.operation_id, publications, context=changed.encode()
    )
    with pytest.raises(ValueError):
        StartPlanFacts.from_plan(incoming, wrong)


@pytest.mark.parametrize(
    "mutation", ["trailing", "duplicate", "unknown", "version", "empty", "oversized"]
)
def test_start_context_rejects_ambiguous_noncanonical_or_unsupported_json(mutation):
    _, facts, _ = facts_fixture()
    raw = facts.encode()
    prefix = raw[: raw.index(b"\0") + 1]
    body = raw[len(prefix) :]
    if mutation == "trailing":
        raw += b" "
    elif mutation == "duplicate":
        raw = prefix + b'{"at":"other",' + body[1:]
    elif mutation == "unknown":
        raw = prefix + b'{"unknown":null,' + body[1:]
    elif mutation == "version":
        raw = raw.replace(b"/v1\0", b"/v9\0", 1)
    elif mutation == "empty":
        raw = prefix + b"{}"
    else:
        raw = b"x" * 16385
    with pytest.raises(ValueError):
        StartPlanFacts.decode(raw)


@pytest.mark.parametrize("change", [{"at": datetime(2026, 10, 21)}, {"input_hash": b"short"}])
def test_start_context_requires_aware_clock_and_original_hash(change):
    _, facts, _ = facts_fixture()
    with pytest.raises(ValueError):
        replace(facts, **change)


def test_configuration_cannot_bind_other_session_or_evidence():
    _, facts, _ = facts_fixture()
    for field in ("session_id", "evidence_set_id"):
        with pytest.raises(ValueError):
            replace(facts, configuration=replace(facts.configuration, **{field: str(uuid4())}))


@pytest.mark.parametrize(
    "selector", ["invalid", "[]", '{"x":1, "y":2}', '{"x":1,"x":2}', '{"x":NaN}']
)
def test_start_facts_configuration_selector_is_canonical_and_unambiguous(selector):
    _, facts, _ = facts_fixture()
    with pytest.raises(ValueError):
        replace(facts, configuration=replace(facts.configuration, selector_query=selector))
