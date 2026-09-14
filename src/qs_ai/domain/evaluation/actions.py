"""Automatic evaluation planning; manual recovery needs a separate audited resolution."""

import re
from dataclasses import dataclass

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.policy import ExecutionPolicy


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    failure: ClassifiedFailure | None = None
    replacement_authorized: bool = False
    contract_recovery_authorized: bool = False

    def __post_init__(self) -> None:
        if type(self.replacement_authorized) is not bool or (
            self.replacement_authorized and self.status != "result_unknown"
        ):
            raise ValueError("Replacement authorization requires unknown evidence")
        if type(self.contract_recovery_authorized) is not bool or (
            self.contract_recovery_authorized
            and (
                self.status != "failed"
                or self.failure is None
                or self.failure.code != "semantic_decision_contract_invalid"
                or not self.failure.allows_semantic_retry()
            )
        ):
            raise ValueError("Contract recovery requires a semantic contract failure")
        if self.status not in ("succeeded", "failed", "result_unknown"):
            raise ValueError("Invalid terminal execution status")
        if self.status == "succeeded" and self.failure is not None:
            raise ValueError("Successful execution cannot carry failure")


@dataclass(frozen=True)
class CandidateProgress:
    candidate_id: str
    review_ready: bool
    semantic: tuple[ExecutionResult, ...] = ()

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.candidate_id):
            raise ValueError("Invalid candidate identity")
        if type(self.review_ready) is not bool or not isinstance(self.semantic, tuple):
            raise ValueError("Invalid candidate progress")
        if self.review_ready and not any(x.status == "succeeded" for x in self.semantic):
            raise ValueError("Review ready candidate requires successful semantic evidence")


@dataclass(frozen=True)
class SlotProgress:
    case_id: str
    ordinal: int
    generation: tuple[ExecutionResult, ...] = ()
    candidate: CandidateProgress | None = None

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.case_id)
            or type(self.ordinal) is not int
            or self.ordinal < 1
        ):
            raise ValueError("Invalid slot identity")
        if not isinstance(self.generation, tuple):
            raise ValueError("Immutable generation history required")
        if self.candidate is not None and not any(x.status == "succeeded" for x in self.generation):
            raise ValueError("Candidate requires successful generation evidence")


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
    if (
        not isinstance(slots, tuple)
        or len(slots) != policy.generation_cases * policy.candidates_per_case
    ):
        raise ValueError("Incomplete frozen slot plan")
    seen_cases: set[str] = set()
    seen_candidates: set[str] = set()
    for offset in range(0, len(slots), policy.candidates_per_case):
        group = slots[offset : offset + policy.candidates_per_case]
        case_id = group[0].case_id
        if case_id in seen_cases or any(
            slot.case_id != case_id or slot.ordinal != ordinal
            for ordinal, slot in enumerate(group, 1)
        ):
            raise ValueError("Duplicate or disordered frozen slots")
        seen_cases.add(case_id)
        for slot in group:
            if slot.candidate is not None:
                if slot.candidate.candidate_id in seen_candidates:
                    raise ValueError("Candidate belongs to more than one slot")
                seen_candidates.add(slot.candidate.candidate_id)
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
    # Each authorization must be projected from a persisted resolution bound to
    # the exact unknown execution, never inferred from the aggregate count.
    historical_unknown = any(
        execution.status == "result_unknown" and not execution.replacement_authorized
        for slot in slots
        for execution in (*slot.generation, *(slot.candidate.semantic if slot.candidate else ()))
    )
    if unresolved_unknown or historical_unknown:
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
        if last.status == "result_unknown" and not last.replacement_authorized:
            return action("block", "result_unknown_requires_review")
        limit = policy.generation_per_slot if candidate is None else policy.semantic_per_candidate
        if ordinal > limit:
            return action("block", kind + "_budget_exhausted")
        if last.contract_recovery_authorized:
            if kind != "semantic":
                raise ValueError("Contract recovery cannot regenerate candidates")
            return action(kind, "semantic_contract_recovery_approved")
        if last.replacement_authorized:
            return action(kind, "manual_recovery_approved")
        allowed = (
            policy.allows_automatic_generation_recovery
            if candidate is None
            else policy.allows_automatic_semantic_recovery
        )
        if last.failure is None or not allowed(last.failure):
            return action("block", kind + "_recovery_not_allowed")
        return action(kind, kind + "_recovery_allowed")
    return NextAction("await_review", "candidate_evidence_complete")
