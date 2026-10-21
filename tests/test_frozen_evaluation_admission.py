"""Frozen evaluation source/clock and original root identity are checked before business."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.transaction import BindingError

from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.workflow_transport import frozen_evaluation_admission as original
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.frozen_evaluation_events import FrozenEvaluationEvents
from tests.test_evaluation_admission_planner import fixture as planning


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    plan = await f.planner.candidate(f.incoming)
    base = WorkflowCommandAdmission(*(SimpleNamespace() for _ in range(6)))
    action = original.FrozenEvaluationAdmission(f.incoming, plan, base)
    db = f.read.db
    db.info = {"state_events": FrozenEvaluationEvents(f.incoming, plan)}
    db.scalar = AsyncMock(return_value=f.read.f.at.replace(tzinfo=None))
    checkpoint = Mock()
    checkpoint.mappings.return_value.one_or_none.return_value = {
        "version": 1,
        "checkpoint_json": None,
    }
    source = Mock()
    source.mappings.return_value.one_or_none.return_value = f.read.source
    db.execute = AsyncMock(side_effect=[checkpoint, source])
    lock = AsyncMock()
    monkeypatch.setattr(original, "lock_admission", lock)
    action._admission.admit = AsyncMock(return_value="original decision")
    return SimpleNamespace(**locals())


async def test_original_evaluation_uses_current_checkpoint_run_locks_and_frozen_clock(monkeypatch):
    f = await fixture(monkeypatch)
    assert (
        await f.action(f.db, f.f.incoming.envelope(), f.f.incoming.body_message())
        == "original decision"
    )
    f.lock.assert_awaited_once_with(f.db, 7)
    assert all(c.args[0]._for_update_arg is not None for c in f.db.execute.await_args_list)
    assert f.action._admission._clock() == f.f.read.f.at
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["source", "version", "clock", "recorder"])
async def test_changed_source_or_missing_original_transcription_does_not_enter_business(
    monkeypatch, damage
):
    f = await fixture(monkeypatch)
    if damage == "source":
        f.f.read.source["progress_json"] = {"status": "canceled"}
    elif damage == "version":
        f.checkpoint.mappings.return_value.one_or_none.return_value["version"] = 2
    elif damage == "clock":
        f.db.scalar.return_value += timedelta(days=1)
    else:
        f.db.info.clear()
    with pytest.raises((BindingError, ConfigurationPlanConflict)):
        await f.action(f.db, f.f.incoming.envelope(), f.f.incoming.body_message())
    f.action._admission.admit.assert_not_called()
