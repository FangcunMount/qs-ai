"""Fence candidate ownership, then aggregate only after durable completion is accepted."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import candidate_plan
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    active_claims,
    lock_run,
    release_claim,
    require_claim,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs


async def completion_owner(
    db: AsyncSession,
    run_id: UUID,
    organization_id: int,
    claim: SlotClaim,
) -> tuple[dict, int, SlotClaim]:
    if claim.run_id != run_id:
        raise CheckpointConflict("Claim belongs to another Run")
    run, version = await lock_run(db, run_id, organization_id)
    await active_claims(db, run_id)
    current = await require_claim(db, claim)
    if not run["progress_json"] or run["progress_json"]["status"] not in ("collecting", "blocked"):
        raise CheckpointConflict("Run no longer accepts candidate results")
    return run, version, current


async def complete_claim(
    db: AsyncSession,
    run_id: UUID,
    version: int,
    claim: SlotClaim,
    progress: dict,
    actor: str,
    at: datetime,
) -> CheckpointState:
    await release_claim(db, claim)
    active = await active_claims(db, run_id)
    run = dict(
        (
            await db.execute(
                select(evaluation_runs)
                .where(evaluation_runs.c.run_id == str(run_id))
                .with_for_update()
            )
        )
        .mappings()
        .one()
    )
    run["progress_json"] = progress
    # Projection validates all completed records and every other live dispatch.
    plan = await candidate_plan(db, run_id, run, active, 32)
    target, cause = "", ""
    if not active:
        if progress.get("unresolved_result_unknown_count", 0):
            target, cause = "blocked", "result_unknown_requires_review"
        elif not progress.get("cancel_requested"):
            if plan.complete:
                target, cause = "awaiting_review", "candidate_evidence_complete"
            elif plan.blocked and not plan.ready:
                target, cause = "blocked", plan.blocked[0].cause
    at = max([at, *(datetime.fromisoformat(t["at"]) for t in progress["transitions"])])
    if target and target != progress["status"]:
        progress = {
            **progress,
            "status": target,
            "transitions": [
                *progress["transitions"],
                {
                    "from": progress["status"],
                    "to": target,
                    "cause_code": cause,
                    "actor": actor,
                    "at": at.isoformat(),
                    "evidence_refs": [claim.checkpoint.execution_id],
                },
            ],
        }
    state = CheckpointState(run_id, version + 1, None)
    await save_checkpoint(db, state, version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(progress_json=progress)
    )
    return state


async def completed_claim_state(
    db: AsyncSession,
    organization_id: int,
    claim: SlotClaim,
) -> CheckpointState | None:
    """A repeated completion/recovery is a read, never a second candidate insertion."""
    from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode
    from qs_ai.infrastructure.persistence.mysql.schema import (
        evaluation_dispatches,
        evaluation_generation_completions,
        evaluation_semantic_completions,
    )

    _, version = await lock_run(db, claim.run_id, organization_id)
    await active_claims(db, claim.run_id)
    cp = claim.checkpoint
    ledger = (
        await db.execute(
            select(evaluation_dispatches.c.checkpoint_json)
            .where(
                evaluation_dispatches.c.run_id == str(claim.run_id),
                evaluation_dispatches.c.invocation_id == cp.invocation_id,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if ledger is None:
        return None
    original = decode(ledger)
    if original is None or any(
        getattr(original, key) != getattr(cp, key)
        for key in (
            "execution_id",
            "invocation_id",
            "owner",
            "kind",
            "case_id",
            "slot_ordinal",
            "candidate_id",
            "execution_ordinal",
            "dispatch_started_at",
        )
    ):
        raise CheckpointConflict("Completion identity differs from original dispatch")
    table = (
        evaluation_generation_completions
        if cp.kind == "generation"
        else evaluation_semantic_completions
    )
    record = (
        await db.execute(
            select(table.c.execution_id)
            .where(table.c.run_id == str(claim.run_id), table.c.invocation_id == cp.invocation_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if record is None:
        return None
    if record != cp.execution_id:
        raise CheckpointConflict("Completed invocation identity differs")
    return CheckpointState(claim.run_id, version, None)
