"""Virtual failures select existing first evidence without writing or claiming a commit."""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
    candidate_completion_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_records import (
    CandidateTerminalRecords,
    virtual_blocked_execution,
)
from tests.test_candidate_terminal_progress import AT, setup


def records():
    generation = dict(
        execution_id="generation:first",
        invocation_id="generation:call",
        case_id="case:first",
        slot_ordinal=1,
        execution_ordinal=1,
        evidence_json=dict(
            status="result_unknown", finished_at=(AT + timedelta(seconds=1)).isoformat()
        ),
    )
    semantic = dict(
        execution_id="semantic:first",
        invocation_id="semantic:call",
        candidate_id="candidate:first",
        execution_ordinal=1,
        evidence_json=dict(
            status="result_unknown", finished_at=(AT + timedelta(seconds=2)).isoformat()
        ),
    )
    return CandidateTerminalRecords((), (), (generation,), (semantic,))


@pytest.mark.parametrize("resolved", [False, True])
def test_virtual_unknown_uses_first_unresolved_terminal_across_both_kinds(resolved):
    view = records()
    progress = dict(
        result_unknown_resolutions=[dict(execution_id="generation:first")] if resolved else []
    )
    actual = virtual_blocked_execution(view, progress, "result_unknown_requires_review", ())
    assert actual == (
        "semantic:first" if resolved else "generation:first",
        AT + timedelta(seconds=2 if resolved else 1),
    )


@pytest.mark.parametrize("kind", ["generation", "semantic"])
def test_virtual_budget_block_keeps_original_kind_candidate_slot_and_ordinal(kind):
    view = records()
    cause = kind + "_budget_exhausted"
    action = SimpleNamespace(
        cause=cause,
        execution_ordinal=2,
        case_id="case:first",
        slot_ordinal=1,
        candidate_id="candidate:first",
    )
    actual = virtual_blocked_execution(view, {}, cause, (action,))
    assert actual == (kind + ":first", AT + timedelta(seconds=1 if kind == "generation" else 2))


@pytest.mark.parametrize(
    "damage",
    [
        "resolved_all",
        "missing_action",
        "wrong_cause",
        "unsupported",
        "wrong_ordinal",
        "wrong_candidate",
    ],
)
def test_virtual_evidence_refuses_missing_or_unrelated_failure(damage):
    view = records()
    cause = "semantic_budget_exhausted"
    action = SimpleNamespace(cause=cause, execution_ordinal=2, candidate_id="candidate:first")
    progress = {}
    blocked = (action,)
    if damage == "resolved_all":
        cause = "result_unknown_requires_review"
        progress = dict(
            result_unknown_resolutions=[
                dict(execution_id=k) for k in ("generation:first", "semantic:first")
            ]
        )
    elif damage == "missing_action":
        blocked = ()
    elif damage == "wrong_cause":
        action.cause = "foreign"
    elif damage == "unsupported":
        cause = action.cause = "foreign"
    elif damage == "wrong_ordinal":
        action.execution_ordinal = 1
    elif damage == "wrong_candidate":
        action.candidate_id = "candidate:foreign"
    with pytest.raises(CheckpointConflict):
        virtual_blocked_execution(view, progress, cause, blocked)


@pytest.mark.parametrize("scenario", ["last_unknown", "semantic_recovery", "active_unknown"])
async def test_virtual_progress_uses_proposed_evidence_without_reading_or_releasing_ownership(
    monkeypatch, scenario
):
    p = setup(monkeypatch, scenario)
    result = await candidate_completion_progress(
        p.db,
        p.p.p.run_id,
        p.p.claim,
        p.progress,
        "worker:first",
        AT + timedelta(seconds=4),
        active=p.active,
        plan=p.plan,
        blocked_evidence=("execution:proposed", AT + timedelta(seconds=2)),
    )
    assert result["status"] == ("collecting" if scenario == "active_unknown" else "blocked")
    if scenario != "active_unknown":
        assert result["transitions"][-1]["evidence_refs"] == ["execution:proposed"]
    p.blocked_read.assert_not_called()
    p.db.execute.assert_not_called()
    p.release.assert_not_called()
    p.save.assert_not_called()
