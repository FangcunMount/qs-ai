"""Whole START candidates freeze outside the read transaction; no Broker is involved."""

import hashlib
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.wire import Envelope, encode

from qs_ai.application.interpretation.ports import Receipt
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.contracts.workflow.command_mapping import start_arguments
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql import start_admission as reads
from qs_ai.infrastructure.persistence.mysql.execution_configurations import PlannedConfiguration
from qs_ai.infrastructure.workflow_transport import start_planner as planning
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    METADATA,
    SCHEMA,
    PreparedMessage,
)
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.test_input_binding import bound_case
from tests.test_transaction_command import command_fixture


def fixture(monkeypatch, kind=pb.START):
    envelope, raw, wire = command_fixture(kind)
    body = pb.MessagingBody.FromString(raw)
    if kind == pb.START:
        body.start.testee_id = "7"
        body.start.assessment_ids[:] = ["42"]
        body.start.evidence.extend(
            [workflow.EvidenceItem(**asdict(item)) for item in bound_case()[1].items]
        )
        raw = body.SerializeToString(deterministic=True)
        envelope.body_sha256, envelope.body_length, envelope.inline_body = (
            hashlib.sha256(raw).hexdigest(),
            len(raw),
            raw,
        )
    incoming = FrozenCommandInput.from_authenticated(envelope, raw, wire)
    reading = False
    opened = []
    selected = Mock()
    selected.mappings.return_value.one_or_none.return_value = None
    db = SimpleNamespace(
        execute=AsyncMock(return_value=selected), scalar=AsyncMock(), connection=AsyncMock()
    )

    @asynccontextmanager
    async def read_session():
        nonlocal reading
        reading = True
        opened.append(True)
        try:
            yield db
        finally:
            reading = False

    tx = SimpleNamespace(open=read_session)
    ids = []
    at = datetime(2026, 10, 21, tzinfo=UTC)
    clock = Mock(return_value=at)

    def new_id():
        assert not reading
        value = str(uuid4())
        ids.append(value)
        return value

    def first_wire(
        kind,
        identity,
        aggregate,
        body,
        *,
        organization_id,
        signing_key,
        recipient_key,
        correlation="",
        original_occurred_at="",
    ):
        assert not reading, "all signing/freezing must follow read transaction cleanup"
        raw = body.SerializeToString(deterministic=True)
        header = pb.MessagingEnvelope(
            schema_version=SCHEMA,
            producer="qs-ai",
            destination="qs-server",
            message_id=identity,
            kind=kind,
            aggregate_key=aggregate,
            correlation_command_id=correlation,
            original_occurred_at=original_occurred_at,
            body_sha256=hashlib.sha256(raw).hexdigest(),
            body_length=len(raw),
            inline_body=raw,
        )
        return PreparedMessage(
            header,
            raw,
            encode(Envelope(identity, b"opaque-first-" + identity.encode(), dict(METADATA))),
            EVENTS,
        )

    prepared = Mock(side_effect=first_wire)
    monkeypatch.setattr(planning, "prepare", prepared)
    publication_id = str(uuid4())

    async def configuration(db, session, evidence, models):
        return PlannedConfiguration(
            session.id,
            evidence.id,
            evidence.fingerprint,
            publication_id,
            "a" * 64,
            1,
            '{"organization_id":7}',
        )

    config = AsyncMock(side_effect=configuration)
    monkeypatch.setattr(reads, "plan_configuration", config)
    monkeypatch.setattr(reads, "preview_capacity", AsyncMock())
    planner = planning.StartAdmissionPlanner(
        tx, SimpleNamespace(), signing_key=Mock(), recipient_key=Mock(), clock=clock, new_id=new_id
    )
    return SimpleNamespace(**locals())


async def test_constructor_is_inert_and_candidate_freezes_complete_first_messages_after_reads(
    monkeypatch,
):
    f = fixture(monkeypatch)
    assert not f.opened and not f.ids
    f.clock.assert_not_called()
    f.prepared.assert_not_called()
    plan = await f.planner.candidate(f.incoming)
    facts = StartPlanFacts.from_plan(f.incoming, plan)
    values = read_admission_plan(f.incoming, plan)
    assert len(values) == 2 and len(f.ids) == 8 and f.clock.call_count == 1
    assert facts.configuration is not None and facts.at == f.at
    assert facts.session_id in f.ids and facts.job_id in f.ids
    receipt = pb.MessagingBody.FromString(values[0].body).command_receipt
    assert receipt.decision == pb.ACCEPTED and receipt.workflow_receipt.status == "queued"
    assert receipt.workflow_receipt.session_id == facts.session_id
    assert len(f.opened) == 1 and f.prepared.call_count == 2


async def test_configuration_refusal_freezes_original_blocked_state_and_receipt(monkeypatch):
    f = fixture(monkeypatch)
    f.config.side_effect = RuleViolation("configuration_unavailable")
    plan = await f.planner.candidate(f.incoming)
    facts = StartPlanFacts.from_plan(f.incoming, plan)
    values = read_admission_plan(f.incoming, plan)
    receipt = pb.MessagingBody.FromString(values[0].body).command_receipt
    state = pb.MessagingBody.FromString(values[1].body).interpretation_state
    assert receipt.decision == pb.REJECTED and receipt.grpc_status_code == 9
    assert receipt.code == state.failure_code == "configuration_unavailable"
    assert state.version == receipt.workflow_receipt.version == 4
    assert facts.configuration is None


async def test_previous_original_receipt_reuses_business_identity_without_new_state(monkeypatch):
    f = fixture(monkeypatch)
    actor, testee, assessments, goal, _, evidence = start_arguments(f.incoming.body_message().start)
    prior = Receipt(str(uuid4()), str(uuid4()), "queued", 2)
    f.selected.mappings.return_value.one_or_none.return_value = {
        "request_hash": fingerprint(
            [asdict(actor), testee, assessments, goal] + [asdict(item) for item in evidence]
        ),
        "response": asdict(prior),
    }
    plan = await f.planner.candidate(f.incoming)
    facts = StartPlanFacts.from_plan(f.incoming, plan)
    assert facts.session_id == prior.session_id and facts.run_id == prior.run_id
    assert len(plan.branches) == 1 and f.prepared.call_count == 1
    f.config.assert_not_called()


async def test_database_unknown_does_not_freeze_a_false_refusal_or_any_publication(monkeypatch):
    f = fixture(monkeypatch)
    f.db.execute.side_effect = ConnectionError("database unavailable")
    with pytest.raises(ConnectionError):
        await f.planner.candidate(f.incoming)
    f.prepared.assert_not_called()


async def test_other_command_kind_is_rejected_before_clock_identity_or_session(monkeypatch):
    f = fixture(monkeypatch, pb.EVALUATION_START)
    with pytest.raises(ValueError):
        await f.planner.candidate(f.incoming)
    assert not f.opened and not f.ids
    f.clock.assert_not_called()
    f.prepared.assert_not_called()


async def test_invalid_original_start_freezes_only_the_original_refusal_without_business_state(
    monkeypatch,
):
    f = fixture(monkeypatch)
    body = f.incoming.body_message()
    body.start.goal = ""
    raw = body.SerializeToString(deterministic=True)
    header = f.incoming.envelope()
    header.body_sha256, header.body_length, header.inline_body = (
        hashlib.sha256(raw).hexdigest(),
        len(raw),
        raw,
    )
    incoming = FrozenCommandInput.from_authenticated(header, raw, f.incoming.wire)
    plan = await f.planner.candidate(incoming)
    facts = StartPlanFacts.from_plan(incoming, plan)
    values = read_admission_plan(incoming, plan)
    assert len(values) == 1 and facts.configuration is None
    decision = pb.MessagingBody.FromString(values[0].body).command_receipt
    assert decision.decision == pb.REJECTED and decision.code == "invalid_session_input"
    assert decision.grpc_status_code == 3 and not decision.HasField("workflow_receipt")
    f.db.execute.assert_not_called()


@pytest.mark.parametrize("available", [False, True])
async def test_previous_blocked_start_requires_exact_original_refusal_evidence(
    monkeypatch, available
):
    f = fixture(monkeypatch)
    actor, testee, assessments, goal, _, evidence = start_arguments(f.incoming.body_message().start)
    prior = Receipt(str(uuid4()), str(uuid4()), "blocked", 4)
    f.selected.mappings.return_value.one_or_none.return_value = {
        "request_hash": fingerprint(
            [asdict(actor), testee, assessments, goal] + [asdict(item) for item in evidence]
        ),
        "response": asdict(prior),
    }
    f.db.scalar.return_value = (
        {
            "session_id": prior.session_id,
            "version": prior.version,
            "status": prior.status,
            "request_id": f.incoming.envelope().message_id,
            "actor": asdict(actor),
            "failure_code": "configuration_unavailable",
        }
        if available
        else None
    )
    if not available:
        with pytest.raises(RuntimeError, match="Original admission refusal evidence unavailable"):
            await f.planner.candidate(f.incoming)
        f.prepared.assert_not_called()
    else:
        plan = await f.planner.candidate(f.incoming)
        facts = StartPlanFacts.from_plan(f.incoming, plan)
        assert len(plan.branches) == 1 and facts.session_id == prior.session_id
        decision = pb.MessagingBody.FromString(
            read_admission_plan(f.incoming, plan)[0].body
        ).command_receipt
        assert decision.code == "configuration_unavailable" and decision.grpc_status_code == 9
    f.config.assert_not_called()
