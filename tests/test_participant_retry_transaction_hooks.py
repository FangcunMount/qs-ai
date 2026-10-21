"""Original retry rule/lock/audit boundaries before adding frozen/read adapters."""

from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.retry import ParticipantRetry
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import Receipt
from qs_ai.domain.interpretation.model import Actor, Session
from qs_ai.infrastructure.persistence.mysql import participant_retries as persistence


def fixture(monkeypatch, previous=None):
    session = Session(
        str(uuid4()),
        Actor("7", "9"),
        "19",
        ("42",),
        "goal",
        workflow_version="qs-published-snapshot-v1",
    )
    session.queue(str(uuid4()))
    session.running()
    session.block("model_not_connected")
    source_run, new_run, request_id = session.active_run_id, str(uuid4()), str(uuid4())
    command = ParticipantRetry(
        DraftScope(7, 10), session.id, str(uuid4()), source_run, 4, "explicit retry", True, 1
    )
    uow = SimpleNamespace(
        reserve=AsyncMock(return_value=previous),
        get=AsyncMock(return_value=session),
        enqueue=AsyncMock(),
        save=AsyncMock(),
        receipt=AsyncMock(),
        commit=AsyncMock(),
    )
    statements = []

    async def execute(statement):
        statements.append(statement)
        if statement.is_select:
            table = statement.get_final_froms()[0].name
            if table == persistence.runs.name:
                row = {"id": source_run, "status": "blocked"}
            elif table == persistence.jobs.name:
                row = {"status": "done"}
            elif table == persistence.model_calls.name:
                row = None
            else:
                raise AssertionError("Unexpected dependency")
            result = Mock()
            result.mappings.return_value.one_or_none.return_value = row
            return result
        assert statement.is_insert and statement.table is persistence.participant_retries
        return Mock()

    async def scalar(statement):
        table = statement.get_final_froms()[0].name
        return {
            persistence.sessions.name: request_id,
            persistence.participant_retries.name: None,
            persistence.execution_configurations.name: session.id,
        }[table]

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=execute), scalar=AsyncMock(side_effect=scalar)
    )

    @asynccontextmanager
    async def open_root():
        yield db

    tx = SimpleNamespace(open=open_root, commit=AsyncMock())
    factory = Mock(return_value=uow)
    monkeypatch.setattr(persistence, "MySQLUnitOfWork", factory)
    allocation = Mock(return_value=UUID(new_run))
    monkeypatch.setattr(persistence, "uuid4", allocation)
    store = persistence.MySQLParticipantRetries(tx, ParticipantCapacityPolicy())
    return SimpleNamespace(**locals())


async def test_original_retry_keeps_locking_reads_audit_and_single_local_commit(monkeypatch):
    f = fixture(monkeypatch)
    result = await f.store.retry(f.command)
    assert result == Receipt(f.session.id, f.new_run, "queued", 5)
    assert len(f.statements) == 4
    assert all(s._for_update_arg is not None for s in f.statements if s.is_select)
    audit = f.statements[-1].compile().params
    assert audit["source_run_id"] == f.source_run and audit["run_id"] == f.new_run
    assert audit["receipt"] == asdict(result) and audit["frozen_request_json"] is None
    f.uow.commit.assert_awaited_once()
    f.uow.get.assert_awaited_once_with(f.session.id, lock=True)
    f.allocation.assert_called_once()


async def test_original_retry_duplicate_has_no_new_run_audit_or_commit(monkeypatch):
    prior = Receipt(str(uuid4()), str(uuid4()), "queued", 5)
    f = fixture(monkeypatch, prior)
    assert await f.store.retry(f.command) == prior
    assert not f.statements
    f.uow.get.assert_not_called()
    f.uow.commit.assert_not_called()
    f.allocation.assert_not_called()
