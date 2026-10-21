"""Original participant retry validation with read-only projection and frozen root hooks."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.execution.retry import RetryParticipant
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql import interpretation_admission as projection
from qs_ai.infrastructure.persistence.mysql import retry_admission as adapter
from qs_ai.infrastructure.persistence.mysql.interpretation import session_values
from qs_ai.infrastructure.persistence.mysql.retry_admission import (
    FrozenParticipantRetries,
    ReadOnlyParticipantRetries,
)
from tests.test_participant_retry_transaction_hooks import fixture as original


def fixture(monkeypatch, *, unknown=False):
    f = original(monkeypatch)
    monkeypatch.setattr(adapter, "MySQLUnitOfWork", f.factory)
    at, event_id, job_id = datetime(2026, 10, 21, tzinfo=UTC), str(uuid4()), str(uuid4())
    old_execute = f.db.execute.side_effect

    async def execute(statement):
        table = statement.get_final_froms()[0].name if statement.is_select else statement.table.name
        if statement.is_select and table in (projection.sessions.name, projection.idempotency.name):
            f.statements.append(statement)
            row = (
                dict(session_values(f.session), request_id=f.request_id)
                if table == projection.sessions.name
                else None
            )
            result = Mock()
            result.mappings.return_value.one_or_none.return_value = row
            return result
        if unknown and statement.is_select and table.endswith("model_calls"):
            f.statements.append(statement)
            result = Mock()
            result.mappings.return_value.one_or_none.return_value = {
                "status": "unknown",
                "request_json": None,
            }
            return result
        return await old_execute(statement)

    f.db.execute.side_effect = execute
    f.db.commit = AsyncMock()
    capacity = AsyncMock()
    monkeypatch.setattr(projection, "preview_capacity", capacity)
    read = ReadOnlyParticipantRetries(
        f.tx, f.store.capacity, at=at, run_id=f.new_run, state_event_id=event_id
    )
    source = SimpleNamespace(authorize=AsyncMock())
    return SimpleNamespace(**locals())


async def test_actual_retry_rules_project_state_and_audit_without_writes_or_locks(monkeypatch):
    f = fixture(monkeypatch)
    receipt = await RetryParticipant(f.read, f.source).execute(f.f.command)
    assert receipt.run_id == f.f.new_run and receipt.version == 5
    assert f.read.state.event_id == f.event_id and f.read.state.request_id == f.f.request_id
    assert f.read.audit_digest is not None and len(f.read.audit_digest) == 32
    assert all(s.is_select and s._for_update_arg is None for s in f.f.statements)
    f.source.authorize.assert_awaited_once_with(f.f.session.actor, "19", ("42",))
    f.f.db.commit.assert_not_called()
    f.f.allocation.assert_not_called()
    f.capacity.assert_awaited_once()
    assert f.f.session.status == "blocked" and f.f.session.version == 4


async def test_unknown_provider_result_still_requires_original_explicit_risk_authorization(
    monkeypatch,
):
    f = fixture(monkeypatch, unknown=True)
    with pytest.raises(RuleViolation, match="participant_retry_unknown_risk_required"):
        await RetryParticipant(f.read, f.source).execute(f.f.command)
    assert f.read.state is None and f.read.audit_digest is None
    assert all(s.is_select and s._for_update_arg is None for s in f.f.statements)
    f.capacity.assert_not_called()


async def test_frozen_retry_root_reuses_exact_run_job_clock_and_original_commit_owner(monkeypatch):
    f = fixture(monkeypatch)
    await RetryParticipant(f.read, f.source).execute(f.f.command)
    f.f.statements.clear()
    root = FrozenParticipantRetries(
        f.f.tx,
        f.f.store.capacity,
        at=f.at,
        run_id=f.f.new_run,
        job_id=f.job_id,
        state_event_id=f.event_id,
        expected_audit=f.read.audit_digest,
    )
    result = await root.retry(f.f.command)
    assert result.run_id == f.f.new_run
    assert f.f.factory.call_args.kwargs["commit"] is f.f.tx.commit
    assert f.f.factory.call_args.kwargs["job_id"] == f.job_id
    assert f.f.factory.call_args.kwargs["state_event_id"] == f.event_id
    assert f.f.factory.call_args.kwargs["at"] == f.at
    f.f.allocation.assert_not_called()
    assert sum(s.is_insert for s in f.f.statements) == 1


async def test_changed_source_audit_cannot_be_substituted_into_original_frozen_retry(monkeypatch):
    from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
        ConfigurationPlanConflict,
    )

    f = fixture(monkeypatch)
    root = FrozenParticipantRetries(
        f.f.tx,
        f.f.store.capacity,
        at=f.at,
        run_id=f.f.new_run,
        job_id=f.job_id,
        state_event_id=f.event_id,
        expected_audit=b"x" * 32,
    )
    with pytest.raises(ConfigurationPlanConflict):
        await root.retry(f.f.command)
    f.f.uow.commit.assert_not_called()
    assert not any(s.is_insert for s in f.f.statements)
