"""Original cancellation planning preserves drain/unknown audit and never stages writes."""

import copy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql import evaluation_cancellation as cancellation


def fixture(monkeypatch, *, draining=False, candidate=False):
    scope, at = ManagementScope(uuid4(), 7, 9), datetime(2026, 10, 21, tzinfo=UTC)
    checkpoint = {"run_id": str(scope.run_id), "version": 1, "checkpoint_json": None}
    progress = {
        "status": "collecting" if draining else "requested",
        "transitions": [],
        "unresolved_result_unknown_count": 1 if draining else 0,
    }
    run = {
        "run_id": str(scope.run_id),
        "organization_id": 7,
        "progress_json": progress,
        "execution_mode": "candidate_v2" if candidate else "serial_v1",
    }
    record = {
        "actor": "user:9",
        "reason": "explicit",
        "discard": False,
        "canceled_at": at.isoformat(),
        "execution_id": "",
        "source_status": progress["status"],
    }
    recorder = AsyncMock(return_value=record)
    monkeypatch.setattr(cancellation, "record_for", recorder)
    active = AsyncMock(return_value=())
    monkeypatch.setattr(cancellation, "active_claims", active)

    def selected(row):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = row
        return result

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[selected(checkpoint), selected(run), Mock()]),
        in_transaction=Mock(return_value=False),
        connection=AsyncMock(),
        commit=AsyncMock(),
    )
    return SimpleNamespace(**locals())


async def test_terminal_projection_reuses_original_cancellation_audit_without_writes(monkeypatch):
    f = fixture(monkeypatch)
    before = copy.deepcopy((f.checkpoint, f.run))
    state, updated = await cancellation.plan_cancellation(
        f.db, f.scope, 1, "explicit", f.at, discard=False, confirm=True
    )
    assert state.version == 2 and state.checkpoint is None
    assert (
        updated["status"] == "canceled"
        and updated["cancellation"] == f.record
        and updated["canceled_checkpoint"] is None
    )
    assert (f.checkpoint, f.run) == before
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in f.db.execute.await_args_list
    )
    f.db.connection.assert_not_called()
    f.db.commit.assert_not_called()


async def test_unknown_work_only_plans_original_drain_intent_without_terminal_cancellation(
    monkeypatch,
):
    f = fixture(monkeypatch, draining=True, candidate=True)
    state, updated = await cancellation.plan_cancellation(
        f.db, f.scope, 1, "explicit", f.at, discard=False, confirm=True
    )
    assert state.version == 2 and updated["status"] == "collecting"
    assert (
        updated["cancel_requested"]["status"] == "cancel_requested"
        and "cancellation" not in updated
    )
    f.recorder.assert_not_called()
    f.active.assert_awaited_once_with(f.db, f.scope.run_id, lock=False)
    f.db.commit.assert_not_called()


async def test_pending_unknown_cannot_be_silently_discarded_during_plan(monkeypatch):
    f = fixture(monkeypatch, draining=True)
    with pytest.raises(CheckpointConflict):
        await cancellation.plan_cancellation(
            f.db, f.scope, 1, "explicit", f.at, discard=True, confirm=True
        )
    assert all(c.args[0].is_select for c in f.db.execute.await_args_list)
    f.recorder.assert_not_called()


async def test_original_cancel_keeps_locked_reads_root_cas_and_default_transaction_setup(
    monkeypatch,
):
    f = fixture(monkeypatch)
    save = AsyncMock()
    monkeypatch.setattr(cancellation, "save_checkpoint", save)
    await cancellation.cancel(f.db, f.scope, 1, "explicit", f.at, discard=False, confirm=True)
    assert save.await_count == 1 and save.await_args.args[1].version == 2
    assert all(c.args[0]._for_update_arg is not None for c in f.db.execute.await_args_list[:2])
    assert f.db.execute.await_args.args[0].is_update
    f.db.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )
    f.db.commit.assert_not_called()
