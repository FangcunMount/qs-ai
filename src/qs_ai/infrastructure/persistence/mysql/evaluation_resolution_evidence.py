"""Shared original evidence for unknown-call inspection and authorized resolution."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution, UnknownExecution
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import project_slots
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


def decode_resolutions(values: list[dict]) -> tuple[ResultUnknownResolution, ...]:
    return tuple(
        ResultUnknownResolution(**{**v, "resolved_at": datetime.fromisoformat(v["resolved_at"])})
        for v in values
    )


@dataclass(frozen=True)
class ResolutionEvidence:
    policy: ExecutionPolicy
    generations: list[RowMapping]
    semantics: list[RowMapping]
    dispatches: list[RowMapping]
    unknowns: tuple[UnknownExecution, ...]
    resolutions: tuple[ResultUnknownResolution, ...]


async def load_resolution_evidence(
    db: AsyncSession, run_id: UUID, creation: dict[str, Any], progress: dict[str, Any]
) -> ResolutionEvidence:
    policy = load_execution_policy()
    if creation["execution_policy_json"] != policy.definition_json:
        raise ValueError("Frozen execution policy unavailable")
    records = []
    for table, limit in (
        (evaluation_generation_completions, policy.generation_per_run),
        (evaluation_semantic_completions, policy.semantic_per_run),
        (evaluation_dispatches, policy.generation_per_run + policy.semantic_per_run),
    ):
        rows = list(
            (
                await db.execute(
                    select(table).where(table.c.run_id == str(run_id)).limit(limit + 1)
                )
            ).mappings()
        )
        if len(rows) > limit:
            raise CheckpointConflict("Execution ledger exceeds frozen Run budget")
        records.append(rows)
    generations, semantics, dispatches = records
    # Revalidate original bytes and indexes against every reserved dispatch and prior decision.
    project_slots(
        creation["slots"],
        generations,
        dispatches,
        semantics,
        progress.get("result_unknown_resolutions", []),
    )
    unknowns = tuple(
        UnknownExecution(
            row["execution_id"],
            kind,
            row["execution_ordinal"],
            datetime.fromisoformat(row["evidence_json"]["finished_at"]),
        )
        for kind, rows in (("generation", generations), ("semantic", semantics))
        for row in rows
        if row["evidence_json"]["status"] == "result_unknown"
    )
    prior = decode_resolutions(progress.get("result_unknown_resolutions", []))
    count = progress.get("unresolved_result_unknown_count", 0)
    if type(count) is not int or count != len(unknowns) - len(prior):
        raise ValueError("Unknown execution count differs from persisted evidence")
    if any(r.decision == "cancel_run" for r in prior) and progress["status"] != "canceled":
        raise CheckpointConflict("Canceled resolution differs from Run state")
    return ResolutionEvidence(policy, generations, semantics, dispatches, unknowns, prior)
