"""Correction replay stays before CAS; extracting planning never transfers root ownership."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql import evaluation_review_corrections as corrections
from tests.test_evaluation_review import review


@pytest.mark.parametrize("replay", [False, True])
async def test_original_writer_locks_before_shared_planning_and_only_saves_new_decision(
    monkeypatch, replay
):
    scope = ManagementScope(uuid4(), 7, 42)
    incoming = review()
    result = Mock()
    result.scalar_one_or_none.return_value = 18
    db = SimpleNamespace(
        connection=AsyncMock(), execute=AsyncMock(return_value=result), commit=AsyncMock()
    )
    run = {"version": 18, "run_id": str(scope.run_id), "organization_id": 7}
    lock = AsyncMock(return_value=run)
    updated = (
        None
        if replay
        else {"status": "awaiting_review", "review_corrections": [{"original": True}]}
    )
    planner = AsyncMock(return_value=updated)
    save = AsyncMock()
    monkeypatch.setattr(corrections, "lock_run_in_transaction", lock)
    monkeypatch.setattr(corrections, "plan_review_correction", planner)
    monkeypatch.setattr(corrections, "save_progress", save)
    args = (
        scope,
        17 if replay else 18,
        str(uuid4()),
        "sha256:" + "a" * 64,
        "sha256:" + "b" * 64,
        incoming,
    )
    await corrections.correct_review(db, *args)
    db.connection.assert_awaited_once_with(execution_options={"isolation_level": "REPEATABLE READ"})
    statement = db.execute.await_args.args[0]
    assert statement.is_select and statement._for_update_arg is not None
    lock.assert_awaited_once_with(db, scope, 18)
    planner.assert_awaited_once_with(db, *args, run=run)
    if replay:
        save.assert_not_called()
    else:
        save.assert_awaited_once_with(db, scope, 18, updated)
    db.commit.assert_not_called()


@pytest.mark.parametrize("archived", [False, True])
@pytest.mark.parametrize("damage", [None, "reason", "source", "fingerprint"])
async def test_exact_historical_replay_precedes_current_version_without_new_database_work(
    archived, damage
):
    from dataclasses import replace
    from datetime import timedelta

    from qs_ai.application.evaluation.checkpoints import CheckpointConflict
    from qs_ai.infrastructure.persistence.mysql.evaluation_review_codec import encode_correction
    from tests.test_evaluation_review_correction import correction

    entry = correction()
    scope = ManagementScope(uuid4(), 7, 42)
    stored = [encode_correction(entry)]
    progress = {"status": "approved", "review_corrections": [] if archived else stored}
    if archived:
        progress["review_reopenings"] = [{"previous_review_corrections": stored}]
    run = {"version": entry.version + 5, "progress_json": progress}
    db = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("No new evidence or write")))
    value = replace(entry.review, reviewed_at=entry.review.reviewed_at + timedelta(seconds=5))
    if damage == "reason":
        value = replace(value, reason="Different original intent")
    args = dict(
        scope=scope,
        expected_version=entry.source_version + (1 if damage == "source" else 0),
        command_id=entry.command_id,
        previous_review_fingerprint=(
            "sha256:" + "0" * 64 if damage == "fingerprint" else entry.previous_review_fingerprint
        ),
        candidate_output_fingerprint=entry.candidate_output_fingerprint,
        value=value,
        run=run,
    )
    if damage:
        with pytest.raises(CheckpointConflict):
            await corrections.plan_review_correction(db, **args)
    else:
        assert await corrections.plan_review_correction(db, **args) is None
    db.execute.assert_not_called()
