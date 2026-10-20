"""The original root borrows preallocated state identity, configuration and clock."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import mysql

from qs_ai.domain.interpretation.model import Actor, Session
from qs_ai.infrastructure.persistence.mysql import interpretation, result_outbox
from qs_ai.infrastructure.persistence.mysql.execution_configurations import PlannedConfiguration


@pytest.mark.parametrize("frozen", [False, True])
async def test_state_history_uses_frozen_event_id_without_allocating_inside_root(
    monkeypatch, frozen
):
    identity, request = str(uuid4()), str(uuid4())
    allocate = Mock(return_value=identity)
    if frozen:
        allocate.side_effect = AssertionError("no state identity inside original root")
    monkeypatch.setattr(result_outbox, "uuid4", allocate)
    guard = SimpleNamespace(append=AsyncMock())
    monkeypatch.setattr(result_outbox, "bind", lambda _: guard)
    db = SimpleNamespace(scalar=AsyncMock(return_value=request), info={})
    session = Session(str(uuid4()), Actor("7", "9"), "19", ("42",), "goal")
    session.queue(str(uuid4()))
    kwargs = {"event_id": identity} if frozen else {}
    await result_outbox.stage_state(db, session, **kwargs)
    params = guard.append.await_args.args[0].compile(dialect=mysql.dialect()).params
    assert params["event_id"] == params["payload"]["event_id"] == identity
    assert params["payload"]["request_id"] == request
    if frozen:
        allocate.assert_not_called()
    else:
        allocate.assert_called_once_with()


async def test_original_uow_rechecks_the_exact_frozen_configuration(monkeypatch):
    session = Session(str(uuid4()), Actor("7", "9"), "19", ("42",), "goal")
    evidence = SimpleNamespace(id=str(uuid4()))
    expected = PlannedConfiguration(
        session.id, evidence.id, "a" * 64, str(uuid4()), "b" * 64, 1, "{}"
    )
    bound = AsyncMock()
    monkeypatch.setattr(interpretation, "bind_configuration", bound)
    db = Mock()
    uow = interpretation.MySQLUnitOfWork(db, configuration=expected)
    await uow.bind_configuration(session, evidence)
    bound.assert_awaited_once_with(db, session, evidence, uow.models, expected=expected)


async def test_original_uow_passes_frozen_state_identity_to_history_stage(monkeypatch):
    identity = str(uuid4())
    stage = AsyncMock()
    monkeypatch.setattr(interpretation, "stage_state", stage)
    db = SimpleNamespace(execute=AsyncMock())
    session = Session(str(uuid4()), Actor("7", "9"), "19", ("42",), "goal")
    await interpretation.MySQLUnitOfWork(db, state_event_id=identity).save(session)
    stage.assert_awaited_once_with(db, session, event_id=identity)


@pytest.mark.parametrize("frozen", [False, True])
async def test_original_enqueue_borrows_clock_without_generating_new_time(monkeypatch, frozen):
    class Clock(datetime):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            assert not frozen, "no clock generation inside frozen original root"
            return fixed

    fixed = Clock(2026, 10, 21, tzinfo=UTC)
    monkeypatch.setattr(interpretation, "datetime", Clock)
    reserve = AsyncMock()
    monkeypatch.setattr(interpretation, "reserve_capacity", reserve)
    db = SimpleNamespace(execute=AsyncMock())
    session = Session(
        str(uuid4()),
        Actor("7", "9"),
        "19",
        ("42",),
        "goal",
        workflow_version="qs-published-snapshot-v1",
    )
    session.queue(str(uuid4()))
    uow = (
        interpretation.MySQLUnitOfWork(db, at=fixed)
        if frozen
        else interpretation.MySQLUnitOfWork(db)
    )
    await uow.enqueue(session, None, False, None)
    assert reserve.await_args.args[3] == fixed
    assert Clock.calls == (0 if frozen else 1)
