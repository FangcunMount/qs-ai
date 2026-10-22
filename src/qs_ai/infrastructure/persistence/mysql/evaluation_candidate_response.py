"""Canonical original candidate response identity, independent from renewable ownership."""

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, encode
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    read_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches, evaluation_runs


@dataclass(frozen=True)
class CandidateResponseReference:
    run_id: UUID = field(repr=False)
    organization_id: int = field(repr=False)
    version: int = field(repr=False)
    checkpoint: ExecutionCheckpoint = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, UUID)
            or type(self.organization_id) is not int
            or not 0 < self.organization_id < 2**63
            or type(self.version) is not int
            or not 0 < self.version < 2**63
            or not isinstance(self.checkpoint, ExecutionCheckpoint)
            or self.checkpoint.phase != "dispatching"
        ):
            raise ValueError("Exact original candidate response reference required")

    def fields(self) -> dict[str, Any]:
        return dict(
            run_id=str(self.run_id),
            organization_id=self.organization_id,
            version=self.version,
            checkpoint=encode(self.checkpoint),
        )


@dataclass(frozen=True)
class CandidateResponse:
    reference: CandidateResponseReference = field(repr=False)
    result: EvaluationResponse = field(repr=False)


async def read_candidate_response(
    db: AsyncSession, organization_id: int, claim: SlotClaim
) -> CandidateResponse | None:
    """Read first evidence through original immutable dispatch, even after claim release.

    A renewed lease may allow a valid response after the original dispatch lease
    expires. Neither this immutable reference nor its durable response proves
    live ownership, root commit, model permission or business retry authority.
    Caller owns the read boundary and configured database; this function never
    opens a connection, mutates evidence or commits.
    """
    if not isinstance(claim, SlotClaim):
        raise ValueError("Original candidate claim reference required")
    requested = CandidateResponseReference(
        claim.run_id, organization_id, claim.version, claim.checkpoint
    )
    run = (
        (
            await db.execute(
                select(evaluation_runs.c.run_id, evaluation_runs.c.execution_mode).where(
                    evaluation_runs.c.run_id == str(claim.run_id),
                    evaluation_runs.c.organization_id == organization_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if run is None or run["execution_mode"] != "candidate_v2":
        raise CheckpointConflict("Candidate Run unavailable in organization")
    ledger = (
        await db.execute(
            select(evaluation_dispatches.c.checkpoint_json).where(
                evaluation_dispatches.c.run_id == str(claim.run_id),
                evaluation_dispatches.c.invocation_id == requested.checkpoint.invocation_id,
            )
        )
    ).scalar_one_or_none()
    original = decode(ledger)
    keys = (
        "execution_id",
        "invocation_id",
        "owner",
        "kind",
        "case_id",
        "slot_ordinal",
        "candidate_id",
        "execution_ordinal",
        "phase",
        "claimed_at",
        "dispatch_started_at",
    )
    if original is None or any(getattr(original, k) != getattr(claim.checkpoint, k) for k in keys):
        raise CheckpointConflict("Response identity differs from original candidate dispatch")
    reference = CandidateResponseReference(claim.run_id, organization_id, claim.version, original)
    result = await read_response(db, SlotClaim(claim.run_id, claim.version, original))
    if result is None:
        return None
    if (
        original.dispatch_started_at is None
        or result.finished_at < original.dispatch_started_at
        or result.response is not None
        and result.response.invocation_id != original.invocation_id
    ):
        raise CheckpointConflict("Durable candidate response differs from original dispatch")
    return CandidateResponse(reference, result)
