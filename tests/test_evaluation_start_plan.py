"""Plan the original requested transition without writes, then retain original CAS adapter."""

import copy
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_progress as progress


def fixture():
    run_id, at = uuid4(), datetime(2026, 10, 21, tzinfo=UTC)
    checkpoint = {"run_id": str(run_id), "version": 1, "checkpoint_json": None}
    original = {"status": "requested", "transitions": [], "audit": {"created_at": at.isoformat()}}
    run = {
        "run_id": str(run_id),
        "organization_id": 7,
        "definition_json": json.dumps(original),
        "progress_json": {"status": "requested", "transitions": []},
    }
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("target", ["collecting", "canceled"])
def test_pure_original_transition_preserves_source_and_exact_audit(target):
    f = fixture()
    before = copy.deepcopy((f.checkpoint, f.run))
    state, updated = progress.plan_requested_transition(
        f.run_id, 1, 7, target, "user:9", "explicit transition", f.at, f.checkpoint, f.run
    )
    assert state.run_id == f.run_id and state.version == 2 and state.checkpoint is None
    assert updated["status"] == target
    assert updated["transitions"][-1] == {
        "from": "requested",
        "to": target,
        "actor": "user:9",
        "reason": "explicit transition",
        "cause_code": "evaluation_started" if target == "collecting" else "evaluation_canceled",
        "at": f.at.isoformat(),
    }
    assert ("canceled_at" in updated) is (target == "canceled")
    assert (f.checkpoint, f.run) == before


@pytest.mark.parametrize(
    "wrong", ["version", "checkpoint", "missing-run", "org", "status", "time", "reason", "actor"]
)
def test_pure_transition_retains_original_version_and_audit_refusals(wrong):
    f = fixture()
    at, reason, actor = f.at, "explicit", "user:9"
    if wrong == "version":
        f.checkpoint["version"] = 2
    elif wrong == "checkpoint":
        f.checkpoint["checkpoint_json"] = {"active": True}
    elif wrong == "missing-run":
        f.run = None
    elif wrong == "org":
        f.run["organization_id"] = 8
    elif wrong == "status":
        f.run["progress_json"]["status"] = "collecting"
    elif wrong == "time":
        at -= timedelta(seconds=1)
    elif wrong == "reason":
        reason = " "
    else:
        actor = "untrusted actor"
    with pytest.raises((ValueError, CheckpointConflict)):
        progress.plan_requested_transition(
            f.run_id, 1, 7, "collecting", actor, reason, at, f.checkpoint, f.run
        )


async def test_original_transition_adapter_keeps_locked_reads_and_caller_commit(monkeypatch):
    f = fixture()

    def selected(row):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = row
        return result

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[selected(f.checkpoint), selected(f.run), Mock()]),
        commit=AsyncMock(),
    )
    save = AsyncMock()
    monkeypatch.setattr(progress, "save_checkpoint", save)
    actual = await progress.transition_requested(
        db, f.run_id, 1, 7, "collecting", "user:9", "explicit", f.at
    )
    assert actual.version == 2 and save.await_args.args == (db, actual, 1)
    assert all(call.args[0]._for_update_arg is not None for call in db.execute.await_args_list[:2])
    assert db.execute.await_args.args[0].is_update
    db.commit.assert_not_called()
