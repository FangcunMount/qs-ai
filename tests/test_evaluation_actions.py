from dataclasses import replace

from qs_ai.domain.evaluation.actions import (
    CandidateProgress,
    ExecutionResult,
    SlotProgress,
    next_action,
)
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


def plan():
    return tuple(
        SlotProgress(f"case:{case}", ordinal) for case in range(7) for ordinal in range(1, 6)
    )


def action(slots, **changes):
    values = dict(
        status="collecting",
        preflight_status="passed",
        preflight_case_id="preflight:1",
        slots=slots,
        policy=load_execution_policy(),
    )
    return next_action(**{**values, **changes})


def failure(stage, code, disposition):
    return ClassifiedFailure(
        stage=stage,
        kind="infrastructure_execution",
        code=code,
        retryable=True,
        result_unknown=False,
        disposition=disposition,
        safe_message="retry",
        evidence_refs=("execution:1",),
    )


def test_preflight_and_unknown_precede_slot_selection():
    assert action(plan(), preflight_status="pending").kind == "preflight"
    assert action(plan(), preflight_status="failed").kind == "block"
    assert action(plan(), unresolved_unknown=1).cause == "result_unknown_requires_review"
    assert action(plan(), status="canceled").kind == "none"


def test_failure_never_skips_to_a_later_empty_slot():
    slots = plan()
    blocked = replace(slots[0], generation=(ExecutionResult("failed"),))
    assert action((blocked, *slots[1:])).cause == "generation_recovery_not_allowed"
    unknown = replace(slots[0], generation=(ExecutionResult("result_unknown"),))
    assert action((unknown, *slots[1:])).kind == "block"
    retry = replace(
        slots[0],
        generation=(
            ExecutionResult(
                "failed",
                failure("generation_execution", "provider_rate_limited", "retry_generation"),
            ),
        ),
    )
    result = action((retry, *slots[1:]))
    assert (result.kind, result.case_id, result.slot_ordinal, result.execution_ordinal) == (
        "generation",
        "case:0",
        1,
        2,
    )


def test_existing_candidate_requests_semantic_not_regeneration():
    slots = plan()
    candidate = CandidateProgress("candidate:1", False)
    result = action(
        (
            replace(slots[0], candidate=candidate, generation=(ExecutionResult("succeeded"),)),
            *slots[1:],
        )
    )
    assert result.kind == "semantic" and result.candidate_id == "candidate:1"
    blocked = replace(candidate, semantic=(ExecutionResult("result_unknown"),))
    assert (
        action(
            (
                replace(slots[0], candidate=blocked, generation=(ExecutionResult("succeeded"),)),
                *slots[1:],
            )
        ).kind
        == "block"
    )


def test_invalid_progress_cannot_skip_or_duplicate_candidate_slots():
    import pytest

    with pytest.raises(ValueError, match="Duplicate or disordered"):
        action((plan()[0],) * 35)
    with pytest.raises(ValueError, match="successful semantic"):
        CandidateProgress("candidate:1", True)
    with pytest.raises(ValueError, match="successful generation"):
        replace(plan()[0], candidate=CandidateProgress("candidate:1", False))
    candidate = CandidateProgress("candidate:1", True, (ExecutionResult("succeeded"),))
    slots = tuple(
        replace(slot, generation=(ExecutionResult("succeeded"),), candidate=candidate)
        for slot in plan()
    )
    with pytest.raises(ValueError, match="more than one slot"):
        action(slots)


def test_unknown_in_later_slot_blocks_new_call_even_when_counter_is_zero():
    slots = plan()
    later = replace(slots[-1], generation=(ExecutionResult("result_unknown"),))
    result = action((*slots[:-1], later), unresolved_unknown=0)
    assert result.kind == "block"
    assert result.cause == "result_unknown_requires_review"


def test_unknown_semantic_history_cannot_be_hidden_by_review_ready_flag():
    slots = plan()
    candidate = CandidateProgress(
        "candidate:1",
        True,
        (
            ExecutionResult("result_unknown"),
            ExecutionResult("succeeded"),
        ),
    )
    later = replace(slots[-1], generation=(ExecutionResult("succeeded"),), candidate=candidate)
    assert action((*slots[:-1], later), unresolved_unknown=0).kind == "block"
