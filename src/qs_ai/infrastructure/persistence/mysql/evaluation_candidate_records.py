"""Readonly candidate terminal overlays; never release ownership or prove a commit."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import (
    GenerationAcceptance,
    generation_terminal_values,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import (
    SemanticAcceptance,
    semantic_terminal_values,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    terminal_dispatches,
)


@dataclass(frozen=True)
class CandidateTerminalRecords:
    active: tuple[SlotClaim, ...] = field(repr=False)
    dispatches: tuple[Any, ...] = field(repr=False)
    generations: tuple[Any, ...] = field(repr=False)
    semantics: tuple[Any, ...] = field(repr=False)


def semantic_terminal_records(
    run_id: UUID,
    claim: SlotClaim,
    active: tuple[SlotClaim, ...],
    completion: SemanticCompletion,
    accepted: SemanticAcceptance,
    *,
    dispatches: list[Any],
    generations: list[Any],
    semantics: list[Any],
) -> CandidateTerminalRecords:
    """Overlay an already validated semantic terminal onto one exact candidate claim.

    Caller must first read the scoped owner and original semantic acceptance in
    one snapshot. Original writers repeat those reads under their root locks;
    this data grants no claim, commit, provider or business retry authority.
    """
    cp = claim.checkpoint
    if (
        claim.run_id != run_id
        or cp.kind != "semantic"
        or cp.phase != "dispatching"
        or not completion.matches_checkpoint(
            cp, cp.owner, accepted.generated.normalized_fingerprint
        )
        or (accepted.result is not None) != (completion.status == "succeeded")
        or (accepted.generated.case_id, accepted.generated.slot_ordinal)
        != (cp.case_id, cp.slot_ordinal)
        or accepted.candidate["id"] != cp.candidate_id
        or sum(c == claim for c in active) != 1
        or any(c.run_id != run_id for c in active)
        or len({(c.checkpoint.case_id, c.checkpoint.slot_ordinal) for c in active}) != len(active)
        or any(row["run_id"] != str(run_id) for row in dispatches + generations + semantics)
    ):
        raise CheckpointConflict("Exact scoped candidate terminal source required")
    # Validate all existing live dispatches before removing the completing claim.
    terminal_dispatches(dispatches, generations + semantics, active)
    if any(
        row["execution_id"] == completion.execution_id
        or row["invocation_id"] == completion.invocation_id
        for row in semantics
    ):
        raise CheckpointConflict("Candidate semantic terminal already exists")
    matches = [row for row in generations if row["execution_id"] == accepted.generated.execution_id]
    if (
        len(matches) != 1
        or matches[0]["candidate_id"] != completion.candidate_id
        or not matches[0]["candidate_json"]
        or matches[0]["candidate_json"]["id"] != completion.candidate_id
        or matches[0]["candidate_json"]["generation_execution_id"]
        != accepted.generated.execution_id
        or matches[0]["candidate_json"]["normalized_output_fingerprint"]
        != accepted.generated.normalized_fingerprint
        or matches[0]["candidate_json"]["review_ready"]
    ):
        raise CheckpointConflict("Exact unreviewed generation candidate required")
    updated = tuple(
        {**row, "candidate_json": accepted.candidate}
        if row is matches[0] and accepted.result is not None
        else row
        for row in generations
    )
    return CandidateTerminalRecords(
        tuple(c for c in active if c != claim),
        tuple(dispatches),
        updated,
        (*semantics, semantic_terminal_values(run_id, completion, accepted)),
    )


def virtual_blocked_execution(
    records: CandidateTerminalRecords, progress: dict, cause: str, blocked: tuple
) -> tuple[str, datetime]:
    """Select original block evidence from the exact proposed record view.

    This is planning data. The original writer still reads real durable evidence
    in _blocked_execution; it does not trust a virtual selection as commit proof.
    """
    if cause == "result_unknown_requires_review":
        resolved = {
            value["execution_id"] for value in progress.get("result_unknown_resolutions", [])
        }
        unknowns = [
            row
            for row in (*records.generations, *records.semantics)
            if row["execution_id"] not in resolved
            and row["evidence_json"]["status"] == "result_unknown"
        ]
        if not unknowns:
            raise CheckpointConflict("Unresolved unknown result lacks terminal evidence")
        row = min(unknowns, key=lambda item: item["evidence_json"]["finished_at"])
    else:
        if not blocked or blocked[0].cause != cause:
            raise CheckpointConflict("Blocked candidate differs from projected cause")
        action = blocked[0]
        semantic = cause.startswith("semantic_")
        if not semantic and not cause.startswith("generation_"):
            raise CheckpointConflict("Unsupported candidate block cause")
        source = records.semantics if semantic else records.generations

        def matches(row: Any) -> bool:
            if semantic:
                return bool(row["candidate_id"] == action.candidate_id)
            return (row["case_id"], row["slot_ordinal"]) == (action.case_id, action.slot_ordinal)

        rows = [
            r
            for r in source
            if r["execution_ordinal"] == action.execution_ordinal - 1 and matches(r)
        ]
        if len(rows) != 1 or rows[0]["evidence_json"]["status"] == "succeeded":
            raise CheckpointConflict("Candidate block lacks failed terminal evidence")
        row = rows[0]
    return row["execution_id"], datetime.fromisoformat(row["evidence_json"]["finished_at"])


def generation_terminal_records(
    run_id: UUID,
    claim: SlotClaim,
    active: tuple[SlotClaim, ...],
    completion: GenerationCompletion,
    accepted: GenerationAcceptance,
    *,
    dispatches: list[Any],
    generations: list[Any],
    semantics: list[Any],
) -> CandidateTerminalRecords:
    """Append the original proposed generation terminal; never claim or commit work."""
    cp = claim.checkpoint
    if (
        claim.run_id != run_id
        or cp.kind != "generation"
        or cp.phase != "dispatching"
        or not completion.matches_checkpoint(cp, cp.owner)
        or sum(c == claim for c in active) != 1
        or any(c.run_id != run_id for c in active)
        or len({(c.checkpoint.case_id, c.checkpoint.slot_ordinal) for c in active}) != len(active)
        or any(row["run_id"] != str(run_id) for row in dispatches + generations + semantics)
        or (accepted.candidate is not None) != (completion.status == "succeeded")
    ):
        raise CheckpointConflict("Exact scoped candidate generation source required")
    terminal_dispatches(dispatches, generations + semantics, active)
    if any(
        row["execution_id"] == completion.execution_id
        or row["invocation_id"] == completion.invocation_id
        for row in generations
    ):
        raise CheckpointConflict("Candidate generation terminal already exists")
    if accepted.candidate is not None and (
        accepted.candidate["generation_execution_id"] != completion.execution_id
        or accepted.candidate["normalized_output_fingerprint"] != completion.normalized_fingerprint
        or accepted.candidate["review_ready"]
    ):
        raise CheckpointConflict("Original proposed generation candidate differs")
    return CandidateTerminalRecords(
        tuple(c for c in active if c != claim),
        tuple(dispatches),
        (*generations, generation_terminal_values(run_id, completion, accepted)),
        tuple(semantics),
    )
