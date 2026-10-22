"""Characterize original aggregate owner before sharing its readonly transition rules."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_completion as original
from tests.test_candidate_completion_source import fixture

AT = datetime(2026, 10, 22, tzinfo=UTC)
CASES = (
    ("active", "collecting", ""),
    ("active_unknown", "collecting", ""),
    ("last_unknown", "blocked", "result_unknown_requires_review"),
    ("cancel_unknown", "blocked", "result_unknown_requires_review"),
    ("cancel_complete", "collecting", ""),
    ("last_complete", "awaiting_review", "candidate_evidence_complete"),
    ("last_failure", "blocked", "generation_budget_exhausted"),
    ("ready_and_failed", "collecting", ""),
    ("already_blocked", "blocked", "generation_budget_exhausted"),
    ("semantic_recovery", "blocked", "semantic_recovery_not_allowed"),
    ("late_failure", "blocked", "generation_budget_exhausted"),
)


def setup(monkeypatch, scenario):
    p = fixture()
    progress = dict(
        status="blocked" if scenario == "already_blocked" else "collecting",
        transitions=[dict(at=AT.isoformat())],
    )
    if "unknown" in scenario:
        progress["unresolved_result_unknown_count"] = 1
    if scenario.startswith("cancel"):
        progress["cancel_requested"] = dict(source_version=9, version=10)
    active = (p.claim,) if scenario.startswith("active") else ()
    cause = (
        "semantic_recovery_not_allowed"
        if scenario == "semantic_recovery"
        else "generation_budget_exhausted"
    )
    blocked = (
        () if scenario in ("last_complete", "cancel_complete") else (SimpleNamespace(cause=cause),)
    )
    plan = SimpleNamespace(
        complete=scenario in ("last_complete", "cancel_complete"),
        blocked=blocked,
        ready=(object(),) if scenario == "ready_and_failed" else (),
    )
    release, active_read = AsyncMock(), AsyncMock(return_value=active)
    planner, save = AsyncMock(return_value=plan), AsyncMock()
    failed_at = (
        AT - timedelta(seconds=1) if scenario == "late_failure" else AT + timedelta(seconds=2)
    )
    blocked_read = AsyncMock(return_value=("execution:original-failure", failed_at))
    for key, value in (
        ("release_claim", release),
        ("active_claims", active_read),
        ("candidate_plan", planner),
        ("save_checkpoint", save),
        ("_blocked_execution", blocked_read),
    ):
        monkeypatch.setattr(original, key, value)
    row = Mock()
    row.mappings.return_value.one.return_value = dict(progress_json=dict(status="old-source"))
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[row, Mock()]), commit=AsyncMock(), rollback=AsyncMock()
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("readonly", [False, True])
@pytest.mark.parametrize("scenario,status,cause", CASES)
async def test_original_claim_aggregate_retains_other_dispatch_unknown_cancel_and_failure_time(
    monkeypatch, scenario, status, cause, readonly
):
    p = setup(monkeypatch, scenario)

    async def execute():
        if readonly:
            return await original.candidate_completion_progress(
                p.db,
                p.p.p.run_id,
                p.p.claim,
                p.progress,
                "worker:first",
                AT + timedelta(seconds=4),
                active=p.active,
                plan=p.plan,
            )
        return await original.complete_claim(
            p.db,
            p.p.p.run_id,
            10,
            p.p.claim,
            p.progress,
            "worker:first",
            AT + timedelta(seconds=4),
        )

    if scenario == "late_failure":
        with pytest.raises(CheckpointConflict, match="predates"):
            await execute()
        p.save.assert_not_called()
        assert len(p.db.execute.await_args_list) == (0 if readonly else 1)
        return
    result = await execute()
    if readonly:
        actual = result
    else:
        state = result
        assert state.version == 11 and state.checkpoint is None
        statement = p.db.execute.await_args_list[-1].args[0]
        actual = statement.compile().params["progress_json"]
    assert actual["status"] == status
    changed = bool(cause) and scenario != "already_blocked"
    assert len(actual["transitions"]) == 1 + changed
    if changed:
        event = actual["transitions"][-1]
        assert event["cause_code"] == cause and event["actor"] == "worker:first"
        assert event["evidence_refs"] == (
            ["execution:original-failure"]
            if status == "blocked"
            else [p.p.claim.checkpoint.execution_id]
        )
        expected_at = AT + timedelta(
            seconds=4 if status == "awaiting_review" or scenario == "semantic_recovery" else 2
        )
        assert event["at"] == expected_at.isoformat()
        if scenario == "semantic_recovery":
            assert event["evidence_at"] == (AT + timedelta(seconds=2)).isoformat()
    assert p.progress["status"] == ("blocked" if scenario == "already_blocked" else "collecting")
    assert len(p.progress["transitions"]) == 1
    if readonly:
        p.db.execute.assert_not_called()
        p.release.assert_not_called()
        p.planner.assert_not_called()
        p.save.assert_not_called()
    else:
        assert len(p.db.execute.await_args_list) == 2
        p.release.assert_awaited_once_with(p.db, p.p.claim)
        p.planner.assert_awaited_once()
        p.save.assert_awaited_once_with(p.db, state, 10)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()
