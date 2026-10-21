"""Run original answer/cancel on SELECT-only projections before preparing halves."""

from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.application.interpretation.ports import AccessDenied, Receipt
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.domain.interpretation.model import Actor, Question, RuleViolation, Session, Status
from qs_ai.infrastructure.persistence.mysql import interpretation_admission as reads
from qs_ai.infrastructure.persistence.mysql.interpretation import session_values


def fixture(monkeypatch, *, snapshot=False, previous=None):
    actor = Actor("7", "9")
    session = Session(str(uuid4()), actor, "19", ("42",), "goal")
    session.queue(str(uuid4()))
    session.running()
    question = Question(str(uuid4()), session.id, "question")
    session.await_answer(question.id)
    if snapshot:
        session.workflow_version = "qs-published-snapshot-v1"
    request_id, run_id, event_id = (str(uuid4()) for _ in range(3))
    selected = []

    async def execute(statement):
        selected.append(statement)
        table = statement.get_final_froms()[0].name
        if table == reads.sessions.name:
            row = dict(session_values(session), request_id=request_id)
        elif table == reads.idempotency.name:
            row = (
                None
                if previous is None
                else {"request_hash": previous[0], "response": asdict(previous[1])}
            )
        elif table == reads.questions.name:
            row = asdict(question)
        else:
            raise AssertionError("Unexpected read dependency")
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = row
        return result

    db = SimpleNamespace(execute=AsyncMock(side_effect=execute), commit=AsyncMock())
    capacity = AsyncMock()
    monkeypatch.setattr(reads, "preview_capacity", capacity)
    uow = reads.ReadOnlyInterpretationMutation(
        db, at=datetime(2026, 10, 21, tzinfo=UTC), run_id=run_id, state_event_id=event_id
    )
    source = SimpleNamespace(authorize=AsyncMock())
    service = InterpretationService(uow, source, new_identity=lambda role: run_id)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("action", ["answer", "cancel"])
async def test_actual_change_rules_plan_first_state_without_mutating_database(monkeypatch, action):
    f = fixture(monkeypatch)
    key = str(uuid4())
    if action == "answer":
        receipt = await f.service.answer(
            f.actor, f.session.id, AnswerCommand(4, f.question.id, "yes", False), key
        )
        assert receipt == Receipt(f.session.id, f.run_id, "queued", 5)
    else:
        receipt = await f.service.cancel(f.actor, f.session.id, CancelCommand(4), key)
        assert receipt == Receipt(f.session.id, f.session.active_run_id, "cancelled", 5)
    assert f.uow.state.event_id == f.event_id and f.uow.state.request_id == f.request_id
    assert f.uow.state.version == 5 and not f.uow.state.question_id
    assert f.selected and all(s.is_select and s._for_update_arg is None for s in f.selected)
    f.db.commit.assert_not_called()
    f.source.authorize.assert_awaited_once_with(f.actor, "19", ("42",))
    assert f.session.status == Status.AWAITING_ANSWER and f.question.answered_by is None


async def test_snapshot_answer_still_previews_capacity_and_never_calls_remote_authorization(
    monkeypatch,
):
    f = fixture(monkeypatch, snapshot=True)
    await f.service.answer(
        f.actor, f.session.id, AnswerCommand(4, f.question.id, "yes", False), str(uuid4())
    )
    f.source.authorize.assert_not_called()
    f.capacity.assert_awaited_once()
    planned = f.capacity.await_args.args[1]
    assert planned.active_run_id == f.run_id


@pytest.mark.parametrize("wrong", ["owner", "version", "question", "denied"])
async def test_original_change_refusal_creates_no_state_or_fact(monkeypatch, wrong):
    f = fixture(monkeypatch)
    actor = Actor("7", "10") if wrong == "owner" else f.actor
    version = 3 if wrong == "version" else 4
    question = str(uuid4()) if wrong == "question" else f.question.id
    if wrong == "denied":
        f.source.authorize.side_effect = AccessDenied()
    with pytest.raises((AccessDenied, RuleViolation)):
        await f.service.answer(
            actor, f.session.id, AnswerCommand(version, question, "yes", False), str(uuid4())
        )
    assert f.uow.state is None
    f.db.commit.assert_not_called()


async def test_duplicate_business_change_returns_first_receipt_without_state_or_new_run(
    monkeypatch,
):
    from qs_ai.application.interpretation.service import fingerprint

    command = CancelCommand(4)
    old = Receipt(str(uuid4()), None, "cancelled", 8)
    f = fixture(monkeypatch, previous=(fingerprint(asdict(command)), old))
    result = await f.service.cancel(f.actor, f.session.id, command, str(uuid4()))
    assert result == old and f.uow.state is None
    assert len(f.selected) == 2
    f.db.commit.assert_not_called()
