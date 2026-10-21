"""Original completion owns one commit and never calls a provider during finalization."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.domain.interpretation.model import Actor, Session, Status
from qs_ai.infrastructure.persistence.mysql import execution as module


@pytest.fixture
def original(monkeypatch):
    session = Session(str(uuid4()), Actor("1", "original:actor"), "2", ("3",), "original goal")
    session.queue(str(uuid4()))
    session.running()
    claim = Claim(str(uuid4()), session.active_run_id, session, 7, None, False, None)
    events = []

    async def execute(statement):
        assert statement.is_update or statement.is_insert
        events.append(statement.table.name)

    async def commit():
        events.append("root_commit")

    root = SimpleNamespace(is_active=True)
    db = AsyncMock(spec=AsyncSession)
    db.connection = AsyncMock()
    db.execute = AsyncMock(side_effect=execute)
    db.scalar = AsyncMock(return_value=2)
    db.commit = AsyncMock(side_effect=commit)
    db.get_transaction = Mock(return_value=root)
    db.in_nested_transaction = Mock(return_value=False)
    bound = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(module, "bind", lambda session: bound)

    @asynccontextmanager
    async def opened():
        events.append("open")
        try:
            yield db
        finally:
            events.append("close")

    store = module.MySQLExecutionStore(SimpleNamespace(open=opened))

    async def guard(_db, _claim):
        assert _db is db and _claim is claim
        events.append("original_guard")
        return session

    async def active(_db, _claim):
        events.append("active_lease")

    store._guard = AsyncMock(side_effect=guard)
    store._check_active = AsyncMock(side_effect=active)

    async def save(_self, value):
        assert value is session
        events.append("session_and_original_state")

    monkeypatch.setattr(module.MySQLUnitOfWork, "save", save)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("question", [False, True])
async def test_original_finish_keeps_guard_state_run_job_active_lease_and_commit_order(
    original, question
):
    p = original
    result = WorkflowResult(
        "checkpoint:original",
        question="original question" if question else None,
        failure_code=None if question else "model_not_connected",
    )
    await p.store.finish(p.claim, result)
    expected = (
        ["open", "original_guard"]
        + ([module.questions.name] if question else [])
        + [
            "session_and_original_state",
            module.runs.name,
            module.jobs.name,
            "active_lease",
            module.execution_leases.name,
            "root_commit",
            "close",
        ]
    )
    assert p.events == expected
    assert p.session.status == (Status.AWAITING_ANSWER if question else Status.BLOCKED)
    assert p.session.version == 4
    p.db.commit.assert_awaited_once()


async def test_original_finish_does_not_commit_when_lease_expires_after_business_writes(original):
    p = original
    p.store._check_active.side_effect = LeaseLost("original lease expired")
    with pytest.raises(LeaseLost):
        await p.store.finish(p.claim, WorkflowResult("checkpoint:original"))
    p.db.commit.assert_not_called()
    assert p.events[-1] == "close"


async def test_original_invalid_question_is_refused_before_any_transaction(original):
    p = original
    with pytest.raises(ValueError):
        await p.store.finish(p.claim, WorkflowResult("", question="original question"))
    assert not p.events


async def test_borrowed_finish_stages_first_ids_without_connection_commit_or_new_uuid(
    original, monkeypatch
):
    p = original
    question_id, event_id = str(uuid4()), str(uuid4())

    def no_uuid():
        raise AssertionError("Borrowed finalization cannot generate a question identity")

    monkeypatch.setattr(module, "uuid4", no_uuid)

    async def save(uow, value):
        assert uow._state_event_id == event_id and value.current_question_id == question_id
        p.events.append("session_and_original_state")

    monkeypatch.setattr(module.MySQLUnitOfWork, "save", save)
    result = WorkflowResult("checkpoint:original", question="original frozen question")
    assert (
        await p.store.finish_in(
            p.db, p.claim, result, question_id=question_id, state_event_id=event_id
        )
        is None
    )
    assert p.session.current_question_id == question_id
    p.db.connection.assert_not_called()
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()
    assert p.db.get_transaction() is p.root and p.root.is_active
    assert p.bound.validate.await_count == 2


@pytest.mark.parametrize("fault", ["inactive", "ended", "nested", "handle"])
async def test_borrowed_finish_refuses_wrong_root_before_business(original, fault):
    from reliable_messaging.transaction import BindingError

    p = original
    if fault == "inactive":
        p.root.is_active = False
    elif fault == "ended":
        p.db.get_transaction.return_value = None
    elif fault == "nested":
        p.db.in_nested_transaction.return_value = True
    with pytest.raises(BindingError):
        await p.store.finish_in(
            object() if fault == "handle" else p.db, p.claim, WorkflowResult("checkpoint:original")
        )
    p.store._guard.assert_not_called()
    p.db.commit.assert_not_called()
    assert not p.events


async def test_borrowed_finish_detects_replaced_root_without_claiming_commit(original, monkeypatch):
    from reliable_messaging.transaction import BindingError

    p = original

    async def save(uow, value):
        p.db.get_transaction.return_value = SimpleNamespace(is_active=True)

    monkeypatch.setattr(module.MySQLUnitOfWork, "save", save)
    with pytest.raises(BindingError):
        await p.store.finish_in(p.db, p.claim, WorkflowResult("checkpoint:original"))
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


async def test_borrowed_finish_uses_supplied_first_capacity_release_clock(original, monkeypatch):
    from tests.test_evaluation_resolution import AT

    p = original
    p.session.workflow_version = "qs-snapshot-v1"
    release = AsyncMock()
    monkeypatch.setattr(module, "release_capacity", release)
    await p.store.finish_in(p.db, p.claim, WorkflowResult("checkpoint:original"), at=AT)
    release.assert_awaited_once_with(p.db, p.session, AT)
    p.db.commit.assert_not_called()
