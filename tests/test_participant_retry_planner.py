"""Complete original retry plans reuse authority and frozen source, no model/Broker calls."""

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
from qs_ai.infrastructure.workflow_transport import retry_planner as planning
from qs_ai.infrastructure.workflow_transport.interpretation_plan import InterpretationMutationFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.test_participant_retry_read_plan import fixture as read_fixture
from tests.test_transaction_command import command_fixture


def fixture(monkeypatch, *, unknown=False):
    read = read_fixture(monkeypatch, unknown=unknown)
    envelope, raw, wire = command_fixture(pb.PARTICIPANT_RETRY)
    envelope.aggregate_key = read.f.request_id
    body = pb.MessagingBody.FromString(raw)
    request = body.participant_retry
    request.scope.operator_user_id = 10
    request.session_id, request.expected_run_id = read.f.session.id, read.f.source_run
    (
        request.expected_version,
        request.reason,
        request.confirm,
        request.expected_provider_invocations,
    ) = 4, "explicit retry", True, 1
    raw = body.SerializeToString(deterministic=True)
    envelope.body_sha256, envelope.body_length, envelope.inline_body = (
        hashlib.sha256(raw).hexdigest(),
        len(raw),
        raw,
    )
    incoming = FrozenCommandInput.from_authenticated(envelope, raw, wire)
    active = False

    @asynccontextmanager
    async def open_read():
        nonlocal active
        active = True
        try:
            yield read.f.db
        finally:
            active = False

    read.f.db.connection = AsyncMock()
    read.f.db.get_transaction = Mock(return_value=SimpleNamespace(is_active=True))
    at, ids = datetime(2026, 10, 21, tzinfo=UTC), []

    def identity():
        assert not active
        value = str(uuid4())
        ids.append(value)
        return value

    real_prepare = planning.prepare

    def prepare(*args, **kwargs):
        assert not active, "sign/encrypt after original read cleanup"
        return real_prepare(*args, **kwargs)

    spy = Mock(side_effect=prepare)
    monkeypatch.setattr(planning, "prepare", spy)
    planner = planning.ParticipantRetryPlanner(
        SimpleNamespace(open=open_read),
        read.source,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        clock=lambda: at,
        new_id=identity,
    )
    return SimpleNamespace(**locals())


async def test_original_retry_plan_freezes_complete_first_messages_and_audit_source(monkeypatch):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.incoming)
    facts = InterpretationMutationFacts.from_plan(f.incoming, plan)
    values = read_admission_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(values[0].body).command_receipt
    state = pb.MessagingBody.FromString(values[1].body).interpretation_state
    assert len(f.ids) == 6 and len(values) == 2
    assert (
        facts.kind == pb.PARTICIPANT_RETRY
        and facts.authorization.allowed
        and facts.audit_digest is not None
    )
    assert receipt.decision == pb.ACCEPTED and receipt.workflow_receipt.run_id == facts.run_id
    assert state.event_id == facts.state_event_id and state.version == 5
    f.read.f.allocation.assert_not_called()
    f.read.f.db.commit.assert_not_called()


async def test_missing_unknown_result_risk_authority_freezes_only_original_refusal(monkeypatch):
    f = fixture(monkeypatch, unknown=True)
    plan = await f.planner.candidate(f.incoming)
    facts = InterpretationMutationFacts.from_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(
        read_admission_plan(f.incoming, plan)[0].body
    ).command_receipt
    assert len(plan.branches) == 1 and facts.audit_digest is None
    assert (
        receipt.decision == pb.REJECTED
        and receipt.code == "participant_retry_unknown_risk_required"
    )
    assert facts.authorization.allowed


async def test_known_retry_participant_denial_is_distinct_from_remote_unknown(monkeypatch):
    f = fixture(monkeypatch)
    f.read.source.authorize.side_effect = AccessDenied()
    plan = await f.planner.candidate(f.incoming)
    facts = InterpretationMutationFacts.from_plan(f.incoming, plan)
    assert len(plan.branches) == 1 and not facts.authorization.allowed
    assert facts.audit_digest is None


@pytest.mark.parametrize("where", ["database", "remote"])
async def test_unknown_does_not_authorize_a_retry_or_freeze_a_false_rejection(monkeypatch, where):
    f = fixture(monkeypatch)
    if where == "database":
        f.read.f.db.execute.side_effect = ConnectionError("database unavailable")
    else:
        f.read.source.authorize.side_effect = ConnectionError("authorization unavailable")
    with pytest.raises(ConnectionError):
        await f.planner.candidate(f.incoming)
    f.spy.assert_not_called()
