from dataclasses import replace

import pytest

from qs_ai.domain.evaluation.actions import CandidateProgress, ExecutionResult, SlotProgress
from qs_ai.domain.evaluation.parallel_plan import plan_parallel
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


def plan():
    return tuple(
        SlotProgress(f"case:{case}", ordinal) for case in range(7) for ordinal in range(1, 6)
    )


def choose(slots=None, **changes):
    values = dict(
        status="collecting",
        preflight_status="passed",
        preflight_case_id="preflight:1",
        slots=plan() if slots is None else slots,
        policy=load_execution_policy(),
        active_slots=frozenset(),
        limit=3,
    )
    return plan_parallel(**{**values, **changes})


def test_bounded_selection_excludes_active_slots():
    result = choose(active_slots=frozenset({("case:0", 1)}))
    assert [a.slot_ordinal for a in result.ready] == [2, 3]
    assert not result.complete


def test_candidate_dependency_is_preserved():
    slots = plan()
    first = replace(
        slots[0],
        generation=(ExecutionResult("succeeded"),),
        candidate=CandidateProgress("candidate:1", False),
    )
    result = choose((first, *slots[1:]))
    assert [a.kind for a in result.ready] == ["semantic", "generation", "generation"]
    assert result.ready[0].candidate_id == "candidate:1"


def test_terminal_slot_failure_does_not_hide_independent_work():
    slots = plan()
    first = replace(slots[0], generation=(ExecutionResult("failed"),))
    result = choose((first, *slots[1:]))
    assert len(result.blocked) == 1
    assert [a.slot_ordinal for a in result.ready] == [2, 3, 4]
    assert not result.complete


def test_unknown_anywhere_stops_new_dispatch_without_canceling_active():
    slots = plan()
    last = replace(slots[-1], generation=(ExecutionResult("result_unknown"),))
    result = choose((*slots[:-1], last), active_slots=frozenset({("case:0", 1)}))
    assert result.ready == ()
    assert result.control.cause == "result_unknown_requires_review"
    assert not result.complete


@pytest.mark.parametrize(
    "changes",
    [
        dict(status="canceled"),
        dict(status="blocked"),
        dict(preflight_status="pending"),
        dict(preflight_status="failed"),
    ],
)
def test_run_guards_precede_parallel_selection(changes):
    assert choose(**changes).ready == ()


def test_completion_requires_all_evidence_and_no_active_slot():
    slots = tuple(
        replace(
            s,
            generation=(ExecutionResult("succeeded"),),
            candidate=CandidateProgress(f"candidate:{i}", True, (ExecutionResult("succeeded"),)),
        )
        for i, s in enumerate(plan())
    )
    assert choose(slots).complete
    assert not choose(slots, active_slots=frozenset({("case:0", 1)})).complete


@pytest.mark.parametrize(
    "changes",
    [
        dict(limit=0),
        dict(limit=True),
        dict(limit=33),
        dict(active_slots=frozenset({("missing", 1)})),
        dict(active_slots=set()),
    ],
)
def test_invalid_capacity_or_claim_rejected(changes):
    with pytest.raises(ValueError):
        choose(**changes)


def test_lowered_limit_does_not_create_new_work_or_cancel_existing():
    result = choose(active_slots=frozenset({("case:0", 1), ("case:0", 2)}), limit=1)
    assert not result.ready and not result.complete


def test_exhausted_slots_never_become_review_complete():
    slots = tuple(
        replace(s, generation=(ExecutionResult("failed"), ExecutionResult("failed")))
        for s in plan()
    )
    result = choose(slots)
    assert not result.ready and not result.complete
    assert len(result.blocked) == 35
    assert all(a.cause == "generation_budget_exhausted" for a in result.blocked)


def test_serial_selection_order_is_preserved_at_limit_one():
    from qs_ai.domain.evaluation.actions import next_action

    slots = plan()
    expected = next_action("collecting", "passed", "preflight:1", slots, load_execution_policy())
    assert choose(slots, limit=1).ready == (expected,)
