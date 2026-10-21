"""Governance plans reuse original gates and history without mutating read snapshots."""

import copy
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.finalization import final_record
from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.quality_gates import evaluate_quality_gates
from qs_ai.infrastructure.persistence.mysql import evaluation_finalization as final
from qs_ai.infrastructure.persistence.mysql import evaluation_reopening as reopening
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import GateSnapshot
from tests.test_evaluation_reopening import rejected
from tests.test_quality_gates import complete


def approved():
    inputs = complete()
    scope = ManagementScope(uuid4(), 7, 44)
    preview = GatePreview(
        str(scope.run_id), 9, "sha256:" + "a" * 64, evaluate_quality_gates(**inputs)
    )
    progress = {
        "status": "awaiting_review",
        "transitions": [{"cause_code": "inventory_closed"}],
        "human_reviews": [{"retained": "original decisions"}],
        "frozen_input": {"retained": True},
    }
    return SimpleNamespace(**locals())


def test_finalization_plan_preserves_original_gate_result_audit_and_snapshot():
    f = approved()
    before = copy.deepcopy(f.progress)
    planned = final.finalized_progress(f.progress, f.preview, f.scope.actor, "核对完整门槛", True)
    assert f.progress == before
    assert planned["status"] == "approved"
    record = planned["gate_result"]
    assert record["source_version"] == 9 and record["version"] == 10
    assert record["actor"] == f.scope.actor and record["reason"] == "核对完整门槛"
    assert planned["finalized_at"] == f.preview.quality.evaluated_at.isoformat()
    assert planned["human_reviews"] == before["human_reviews"]
    assert planned["frozen_input"] == before["frozen_input"]
    assert planned["transitions"][:-1] == before["transitions"]
    assert planned["transitions"][-1]["cause_code"] == "human_review_finalized"


def test_changed_expected_gate_does_not_produce_an_approved_plan():
    f = approved()
    before = copy.deepcopy(f.progress)
    with pytest.raises(CheckpointConflict):
        final.finalized_progress(f.progress, f.preview, f.scope.actor, "不符合当前门槛", False)
    assert f.progress == before


async def test_original_finalization_adapter_locks_reads_and_writes_on_caller_root(monkeypatch):
    f = approved()
    run = {"progress_json": f.progress}
    db = SimpleNamespace(commit=AsyncMock())
    lock = AsyncMock(return_value=run)
    read = AsyncMock(return_value=f.preview)
    save = AsyncMock()
    monkeypatch.setattr(final, "lock_run", lock)
    monkeypatch.setattr(final, "evaluate_snapshot", read)
    monkeypatch.setattr(final, "save_progress", save)
    await final.finalize(
        db, f.scope, 9, True, "核对完整门槛", f.preview.quality.evaluated_at, confirm=True
    )
    lock.assert_awaited_once_with(db, f.scope, 9)
    read.assert_awaited_once_with(db, f.scope, run, 9, f.preview.quality.evaluated_at)
    assert save.await_args.args[:3] == (db, f.scope, 9)
    assert save.await_args.args[3]["status"] == "approved"
    db.commit.assert_not_called()


def rejection():
    inputs, options = rejected()
    scope = ManagementScope(uuid4(), 7, 44)
    preview = GatePreview(str(scope.run_id), 9, "sha256:" + "a" * 64, options["gate"])
    snapshot = GateSnapshot(
        preview, options["candidates"], options["reviews"], options["closed_at"]
    )
    record = final_record(preview, scope.actor, "保留原拒绝证据")
    progress = {
        "status": "rejected",
        "gate_result": record,
        "finalized_at": record["finalized_at"],
        "human_reviews": [{"original": "retained signatures"}],
        "transitions": [{"cause_code": "human_review_finalized"}],
        "frozen_input": {"retained": True},
    }
    at = preview.quality.evaluated_at + timedelta(seconds=1)
    return SimpleNamespace(**locals())


def test_reopening_plan_preserves_rejection_history_without_replacing_source():
    f = rejection()
    before = copy.deepcopy(f.progress)
    planned = reopening.reopened_progress(f.scope, "重新核对语义分歧", f.at, f.progress, f.snapshot)
    assert f.progress == before
    assert planned["status"] == "awaiting_review"
    assert planned["gate_result"] is None and planned["finalized_at"] is None
    assert planned["frozen_input"] == before["frozen_input"]
    entry = planned["review_reopenings"][0]
    assert entry["source_version"] == 10 and entry["version"] == 11
    assert entry["previous_finalization"] == before["gate_result"]
    assert entry["candidate_ids"] == ["candidate:0", "candidate:6"]
    assert len(entry["previous_reviews"]) == 70 and len(planned["human_reviews"]) == 66
    assert planned["transitions"][:-1] == before["transitions"]
    assert planned["transitions"][-1]["cause_code"] == "semantic_review_reopened"


@pytest.mark.parametrize("status", ["approved", "awaiting_review", "canceled"])
def test_reopening_projection_cannot_reopen_other_original_states(status):
    f = rejection()
    f.progress["status"] = status
    with pytest.raises((CheckpointConflict, ValueError)):
        reopening.reopened_progress(f.scope, "重新核对语义分歧", f.at, f.progress, f.snapshot)


def test_reopening_retains_corrected_original_reviews_in_round_archive():
    f = rejection()
    f.progress["review_corrections"] = [{"original": "first correction"}]
    before = copy.deepcopy(f.progress)
    planned = reopening.reopened_progress(f.scope, "重新核对语义分歧", f.at, f.progress, f.snapshot)
    entry = planned["review_reopenings"][0]
    assert f.progress == before
    assert entry["previous_original_reviews"] == before["human_reviews"]
    assert entry["previous_review_corrections"] == before["review_corrections"]
    assert planned["review_corrections"] == []
