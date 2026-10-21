"""Run the original START service against a read projection, with no business writes."""

from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.interpretation.ports import Receipt
from qs_ai.application.interpretation.service import InterpretationService, fingerprint
from qs_ai.domain.interpretation.model import Actor, RuleViolation
from qs_ai.infrastructure.persistence.mysql import start_admission as planning
from qs_ai.infrastructure.persistence.mysql.execution_configurations import PlannedConfiguration
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from tests.test_input_binding import bound_case


def fixture(monkeypatch):
    facts = StartPlanFacts(
        b"x" * 32, datetime(2026, 10, 21, tzinfo=UTC), *(str(uuid4()) for _ in range(6))
    )
    selected = Mock()
    selected.mappings.return_value.one_or_none.return_value = None
    db = SimpleNamespace(
        execute=AsyncMock(return_value=selected), commit=AsyncMock(), rollback=AsyncMock()
    )

    publication_id = str(uuid4())

    async def configuration(db, session, evidence, models):
        return PlannedConfiguration(
            session.id,
            evidence.id,
            evidence.fingerprint,
            publication_id,
            "b" * 64,
            1,
            '{"organization_id":1}',
        )

    config = AsyncMock(side_effect=configuration)
    capacity = AsyncMock()
    monkeypatch.setattr(planning, "plan_configuration", config)
    monkeypatch.setattr(planning, "preview_capacity", capacity)
    uow = planning.ReadOnlyStartUnitOfWork(db, facts)
    actor = Actor("1", "2")
    items = bound_case()[1].items
    args = (actor, "7", ("42",), "goal", str(uuid4()), items)
    service = InterpretationService(uow, SimpleNamespace(), new_identity=facts.identity)
    return SimpleNamespace(**locals())


async def test_success_uses_original_service_with_frozen_identities_without_business_sql(
    monkeypatch,
):
    f = fixture(monkeypatch)
    # All allocation is complete before the original service executes the plan.
    import qs_ai.application.interpretation.service as original

    monkeypatch.setattr(original, "uuid4", Mock(side_effect=AssertionError("no new identity")))
    result = await f.service.start_external(*f.args)
    assert result == Receipt(f.facts.session_id, f.facts.run_id, "queued", 2)
    assert f.uow.state.event_id == f.facts.state_event_id
    assert f.uow.state.request_id == f.args[4]
    assert f.uow.state.status == result.status and f.uow.state.version == result.version
    assert f.uow.configuration.session_id == result.session_id
    assert f.uow.configuration.evidence_set_id == f.facts.evidence_id
    assert f.db.execute.await_count == 1 and f.db.execute.await_args.args[0].is_select
    assert f.db.execute.await_args.args[0]._for_update_arg is None
    f.db.commit.assert_not_called()
    f.db.rollback.assert_not_called()
    f.capacity.assert_awaited_once()


@pytest.mark.parametrize("failure", ["configuration", "quota", "input"])
async def test_read_projection_keeps_original_blocked_refusal_and_never_creates_job(
    monkeypatch, failure
):
    f = fixture(monkeypatch)
    args = f.args
    expected = "admission_input_invalid"
    if failure == "configuration":
        f.config.side_effect = RuleViolation("configuration_unavailable")
        expected = "configuration_unavailable"
    elif failure == "quota":
        f.capacity.side_effect = RuleViolation("participant_daily_capacity_exceeded")
        expected = "participant_daily_capacity_exceeded"
    else:
        args = (*f.args[:-1], ())
    result = await f.service.start_external(*args)
    assert result.status == "blocked" and result.version == 4
    assert f.uow.state.failure_code == expected
    assert all(call.args[0].is_select for call in f.db.execute.await_args_list)
    f.db.commit.assert_not_called()


async def test_prior_business_receipt_returns_original_identity_without_replanning_or_allocating(
    monkeypatch,
):
    f = fixture(monkeypatch)
    prior = Receipt(str(uuid4()), str(uuid4()), "queued", 2)
    request_hash = fingerprint(
        [asdict(f.actor), "7", ("42",), "goal"] + [asdict(item) for item in f.items]
    )
    selected = f.db.execute.return_value
    selected.mappings.return_value.one_or_none.return_value = {
        "request_hash": request_hash,
        "response": asdict(prior),
    }
    assert await f.service.start_external(*f.args) == prior
    assert f.uow.previous and f.uow.state is None and f.uow.configuration is None
    f.config.assert_not_called()
    f.capacity.assert_not_called()
    f.db.commit.assert_not_called()


async def test_read_projection_rejects_non_start_write_methods_without_sql(monkeypatch):
    f = fixture(monkeypatch)
    with pytest.raises(RuntimeError):
        await f.uow.cancel_jobs(Mock())
    with pytest.raises(RuntimeError):
        await f.uow.answer(Mock(), f.actor, "answer", False)
    f.db.execute.assert_not_called()
