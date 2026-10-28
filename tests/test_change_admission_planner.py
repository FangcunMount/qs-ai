"""Actual CHANGE rule planning with frozen first wire; no business root or Broker."""

import hashlib
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from jwcrypto import jwk

from qs_ai.application.interpretation.ports import AccessDenied
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport import change_planner as planning
from qs_ai.infrastructure.workflow_transport.interpretation_plan import InterpretationMutationFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.route_fixtures import legacy_routes
from tests.test_read_only_change_admission import fixture as read_fixture
from tests.test_transaction_command import command_fixture


def fixture(monkeypatch, *, snapshot=False, action="answer"):
    read = read_fixture(monkeypatch, snapshot=snapshot)
    envelope, raw, wire = command_fixture(pb.CHANGE)
    envelope.aggregate_key = read.request_id
    body = pb.MessagingBody.FromString(raw)
    body.change.session_id, body.change.action = read.session.id, action
    body.change.expected_version = 4
    body.change.question_id, body.change.answer = read.question.id, "yes"
    raw = body.SerializeToString(deterministic=True)
    envelope.body_sha256, envelope.body_length, envelope.inline_body = (
        hashlib.sha256(raw).hexdigest(),
        len(raw),
        raw,
    )
    incoming = FrozenCommandInput.from_authenticated(envelope, raw, wire, routes=legacy_routes())
    active = False

    @asynccontextmanager
    async def open_read():
        nonlocal active
        active = True
        try:
            yield read.db
        finally:
            active = False

    read.db.connection = AsyncMock()
    at = datetime(2026, 10, 21, tzinfo=UTC)
    ids = []

    def identity():
        assert not active
        value = str(uuid4())
        ids.append(value)
        return value

    real_prepare = planning.prepare

    def prepare(*args, **kwargs):
        assert not active, "signing must follow read transaction cleanup"
        return real_prepare(*args, **kwargs)

    spy = Mock(side_effect=prepare)
    monkeypatch.setattr(planning, "prepare", spy)
    planner = planning.ChangeAdmissionPlanner(
        SimpleNamespace(open=open_read),
        read.source,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        clock=lambda: at,
        new_id=identity,
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("action", ["answer", "cancel"])
async def test_complete_change_plan_uses_original_rules_and_preallocated_resources(
    monkeypatch, action
):
    f = fixture(monkeypatch, action=action)
    assert not f.ids and f.read.db.execute.await_count == 0
    plan = await f.planner.candidate(f.incoming)
    facts = InterpretationMutationFacts.from_plan(f.incoming, plan)
    values = read_admission_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(values[0].body).command_receipt
    state = pb.MessagingBody.FromString(values[1].body).interpretation_state
    assert len(f.ids) == 6 and facts.authorization.allowed
    assert receipt.decision == pb.ACCEPTED and receipt.workflow_receipt.version == 5
    assert state.event_id == facts.state_event_id and state.request_id == f.read.request_id
    assert state.status == ("queued" if action == "answer" else "cancelled")
    if action == "answer":
        assert receipt.workflow_receipt.run_id == facts.run_id
    f.read.db.commit.assert_not_called()
    assert all(s.is_select and s._for_update_arg is None for s in f.read.selected)


async def test_qs_snapshot_does_not_invent_a_remote_authorization_proof(monkeypatch):
    f = fixture(monkeypatch, snapshot=True)
    plan = await f.planner.candidate(f.incoming)
    assert InterpretationMutationFacts.from_plan(f.incoming, plan).authorization is None
    f.read.source.authorize.assert_not_called()


async def test_explicit_remote_denial_freezes_only_original_rejection(monkeypatch):
    f = fixture(monkeypatch)
    f.read.source.authorize.side_effect = AccessDenied()
    plan = await f.planner.candidate(f.incoming)
    facts = InterpretationMutationFacts.from_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(
        read_admission_plan(f.incoming, plan)[0].body
    ).command_receipt
    assert not facts.authorization.allowed and len(plan.branches) == 1
    assert receipt.decision == pb.REJECTED and receipt.grpc_status_code == 7


@pytest.mark.parametrize("failure", ["database", "remote"])
async def test_unknown_dependency_does_not_freeze_a_false_business_refusal(monkeypatch, failure):
    f = fixture(monkeypatch)
    if failure == "database":
        f.read.db.execute.side_effect = ConnectionError("database unavailable")
    else:
        f.read.source.authorize.side_effect = ConnectionError("authorization unavailable")
    with pytest.raises(ConnectionError):
        await f.planner.candidate(f.incoming)
    f.spy.assert_not_called()


async def test_invalid_original_command_has_only_its_durable_rejection_and_no_reads(monkeypatch):
    f = fixture(monkeypatch, action="unsupported")
    plan = await f.planner.candidate(f.incoming)
    facts = InterpretationMutationFacts.from_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(
        read_admission_plan(f.incoming, plan)[0].body
    ).command_receipt
    assert facts.authorization is None and len(plan.branches) == 1
    assert receipt.decision == pb.REJECTED and receipt.grpc_status_code == 3
    f.read.db.execute.assert_not_called()
