"""Automatic evaluation planning; manual recovery needs a separate audited resolution."""

from dataclasses import dataclass

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.policy import ExecutionPolicy


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    failure: ClassifiedFailure | None = None


@dataclass(frozen=True)
class CandidateProgress:
    candidate_id: str
    review_ready: bool
    semantic: tuple[ExecutionResult, ...] = ()


@dataclass(frozen=True)
class SlotProgress:
    case_id: str
    ordinal: int
    generation: tuple[ExecutionResult, ...] = ()
    candidate: CandidateProgress | None = None


@dataclass(frozen=True)
class NextAction:
    kind: str
    cause: str
    case_id: str = ""
    slot_ordinal: int = 0
    candidate_id: str = ""
    execution_ordinal: int = 0
    resume: bool = False


def next_action(
    status: str,
    preflight_status: str,
    preflight_case_id: str,
    slots: tuple[SlotProgress, ...],
    policy: ExecutionPolicy,
    checkpoint: ExecutionCheckpoint | None = None,
    unresolved_unknown: int = 0,
) -> NextAction:
    if status not in (
        "requested",
        "collecting",
        "blocked",
        "awaiting_review",
        "approved",
        "rejected",
        "canceled",
    ):
        raise ValueError("Invalid Run status")
    if type(unresolved_unknown) is not int or unresolved_unknown < 0:
        raise ValueError("Invalid unresolved result count")
    if preflight_status not in ("pending", "passed", "failed"):
        raise ValueError("Invalid preflight status")
    if status == "blocked":
        return NextAction("block", "run_blocked")
    if status != "collecting":
        return NextAction("none", "run_not_collecting")
    if checkpoint is not None:
        return NextAction(
            checkpoint.kind,
            "resume_active_execution",
            checkpoint.case_id,
            checkpoint.slot_ordinal,
            checkpoint.candidate_id,
            checkpoint.execution_ordinal,
            True,
        )
    if unresolved_unknown:
        return NextAction("block", "result_unknown_requires_review")
    if preflight_status != "passed":
        return NextAction(
            "preflight" if preflight_status == "pending" else "block",
            "preflight_" + preflight_status,
            preflight_case_id,
        )
    if len(slots) != policy.generation_cases * policy.candidates_per_case:
        raise ValueError("Incomplete frozen slot plan")
    for slot in slots:
        candidate = slot.candidate
        if candidate is not None and candidate.review_ready:
            continue
        kind = "generation" if candidate is None else "semantic"
        executions = slot.generation if candidate is None else candidate.semantic
        ordinal = len(executions) + 1
        candidate_id = "" if candidate is None else candidate.candidate_id

        def action(
            kind: str,
            cause: str,
            slot: SlotProgress = slot,
            candidate_id: str = candidate_id,
            ordinal: int = ordinal,
        ) -> NextAction:
            return NextAction(kind, cause, slot.case_id, slot.ordinal, candidate_id, ordinal)

        if not executions:
            return action(
                kind, "candidate_missing" if candidate is None else "semantic_evidence_missing"
            )
        last = executions[-1]
        if last.status == "result_unknown":
            return action("block", "result_unknown_requires_review")
        limit = policy.generation_per_slot if candidate is None else policy.semantic_per_candidate
        if ordinal > limit:
            return action("block", kind + "_budget_exhausted")
        allowed = (
            policy.allows_automatic_generation_recovery
            if candidate is None
            else policy.allows_automatic_semantic_recovery
        )
        if last.failure is None or not allowed(last.failure):
            return action("block", kind + "_recovery_not_allowed")
        return action(kind, kind + "_recovery_allowed")
    return NextAction("await_review", "candidate_evidence_complete")
