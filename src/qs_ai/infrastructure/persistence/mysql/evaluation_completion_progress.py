"""Pure original terminal progress for root writes and readonly state planning."""

from collections.abc import Mapping
from typing import Any

from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion


def generation_completion_progress(
    previous: Mapping[str, Any],
    completion: GenerationCompletion,
    policy: ExecutionPolicy,
    owner: str,
    *,
    candidate_claim: bool,
) -> dict[str, Any]:
    progress = dict(previous)
    cause = ""
    if completion.status == "result_unknown":
        progress["unresolved_result_unknown_count"] = (
            progress.get("unresolved_result_unknown_count", 0) + 1
        )
        cause = "result_unknown_requires_review"
    elif completion.status == "failed":
        if completion.execution_ordinal >= policy.generation_per_slot:
            cause = "generation_budget_exhausted"
        elif completion.failure is None or not policy.allows_automatic_generation_recovery(
            completion.failure
        ):
            cause = "generation_recovery_not_allowed"
    # Candidate completion retains the separate original complete_claim owner.
    if cause and not candidate_claim:
        progress["status"] = "blocked"
        progress["transitions"] = [
            *progress["transitions"],
            {
                "from": "collecting",
                "to": "blocked",
                "cause_code": cause,
                "actor": owner.strip(),
                "at": completion.finished_at.isoformat(),
                "evidence_refs": [completion.execution_id],
            },
        ]
    return progress


def semantic_completion_progress(
    previous: Mapping[str, Any],
    completion: SemanticCompletion,
    policy: ExecutionPolicy,
    owner: str,
    *,
    candidate_claim: bool,
) -> dict[str, Any]:
    """Original terminal counters/blocking; review eligibility retains its original owner."""
    progress = dict(previous)
    cause = ""
    if completion.status == "result_unknown":
        progress["unresolved_result_unknown_count"] = (
            progress.get("unresolved_result_unknown_count", 0) + 1
        )
        cause = "result_unknown_requires_review"
    elif completion.status == "failed":
        if completion.execution_ordinal >= policy.semantic_per_candidate:
            cause = "semantic_budget_exhausted"
        elif completion.failure is None or not policy.allows_automatic_semantic_recovery(
            completion.failure
        ):
            cause = "semantic_recovery_not_allowed"
    if cause and not candidate_claim:
        progress["status"] = "blocked"
        progress["transitions"] = [
            *progress["transitions"],
            {
                "from": "collecting",
                "to": "blocked",
                "actor": owner.strip(),
                "cause_code": cause,
                "at": completion.finished_at.isoformat(),
                "evidence_refs": [completion.execution_id],
            },
        ]
    return progress
