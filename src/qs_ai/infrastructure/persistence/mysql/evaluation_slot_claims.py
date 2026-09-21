"""Candidate ownership under the existing Run coordinator; never performs network I/O."""

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, encode
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_runs,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_slot_claims as claims,
)


@dataclass(frozen=True)
class SlotClaim:
    run_id: UUID
    version: int
    checkpoint: ExecutionCheckpoint


def identity(claim: SlotClaim) -> tuple:
    cp = claim.checkpoint
    return (
        claims.c.run_id == str(claim.run_id),
        claims.c.case_id == cp.case_id,
        claims.c.slot_ordinal == cp.slot_ordinal,
    )


async def lock_run(db: AsyncSession, run_id: UUID, organization_id: int) -> tuple[dict, int]:
    coordinator = (
        (
            await db.execute(
                select(evaluation_checkpoints)
                .where(evaluation_checkpoints.c.run_id == str(run_id))
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    run = (
        (
            await db.execute(
                select(evaluation_runs)
                .where(
                    evaluation_runs.c.run_id == str(run_id),
                    evaluation_runs.c.organization_id == organization_id,
                )
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if coordinator is None or run is None or run["execution_mode"] != "candidate_v2":
        raise CheckpointConflict("Candidate Run unavailable in organization")
    if coordinator["checkpoint_json"] is not None:
        raise CheckpointConflict("Candidate Run cannot own a serial checkpoint")
    return dict(run), coordinator["version"]


async def active_claims(db: AsyncSession, run_id: UUID) -> tuple[SlotClaim, ...]:
    rows = (
        (
            await db.execute(
                select(claims)
                .where(claims.c.run_id == str(run_id))
                .order_by(claims.c.case_id, claims.c.slot_ordinal)
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    result = []
    for row in rows:
        cp = decode(row["checkpoint_json"])
        if cp is None or (cp.case_id, cp.slot_ordinal) != (row["case_id"], row["slot_ordinal"]):
            raise CheckpointConflict("Candidate claim index differs from evidence")
        result.append(SlotClaim(run_id, row["version"], cp))
    return tuple(result)


async def require_claim(db: AsyncSession, claim: SlotClaim) -> SlotClaim:
    row = (
        (await db.execute(select(claims).where(*identity(claim)).with_for_update()))
        .mappings()
        .one_or_none()
    )
    cp = decode(row["checkpoint_json"]) if row else None
    if (
        row is None
        or cp is None
        or (cp.case_id, cp.slot_ordinal)
        != (claim.checkpoint.case_id, claim.checkpoint.slot_ordinal)
        or row["version"] != claim.version
        or (cp.execution_id, cp.invocation_id, cp.owner, cp.phase)
        != (
            claim.checkpoint.execution_id,
            claim.checkpoint.invocation_id,
            claim.checkpoint.owner,
            claim.checkpoint.phase,
        )
    ):
        raise CheckpointConflict("Candidate execution ownership changed")
    return SlotClaim(claim.run_id, row["version"], cp)


async def create_claim(
    db: AsyncSession, run_id: UUID, cp: ExecutionCheckpoint, version: int
) -> SlotClaim:
    # The caller holds the Run coordinator; version is monotonic across slot reuse.
    await db.execute(
        insert(claims).values(
            run_id=str(run_id),
            case_id=cp.case_id,
            slot_ordinal=cp.slot_ordinal,
            version=version,
            checkpoint_json=encode(cp),
        )
    )
    return SlotClaim(run_id, version, cp)


async def renew_claim(
    db: AsyncSession, claim: SlotClaim, at: datetime, expires_at: datetime
) -> SlotClaim:
    current = await require_claim(db, claim)
    if (
        at < current.checkpoint.claimed_at
        or at >= current.checkpoint.lease_expires_at
        or expires_at <= current.checkpoint.lease_expires_at
    ):
        raise CheckpointConflict("Candidate lease already expired")
    renewed = replace(current.checkpoint, lease_expires_at=expires_at)
    await db.execute(update(claims).where(*identity(claim)).values(checkpoint_json=encode(renewed)))
    return replace(current, checkpoint=renewed)


async def release_claim(db: AsyncSession, claim: SlotClaim) -> None:
    await require_claim(db, claim)
    await db.execute(delete(claims).where(*identity(claim)))


def terminal_dispatches(
    dispatches: list[Any], completed: list[Any], active: tuple[SlotClaim, ...]
) -> list[Any]:
    """Only hide live dispatches proven by exact claims, never arbitrary missing evidence."""
    terminal = {row["invocation_id"] for row in completed}
    pending = {c.checkpoint.invocation_id: c.checkpoint for c in active}
    filtered = []
    seen = set()
    for row in dispatches:
        invocation = row["invocation_id"]
        if invocation in terminal:
            filtered.append(row)
            continue
        cp = pending.get(invocation)
        original = decode(row["checkpoint_json"])
        if (
            cp is None
            or original is None
            or cp.phase != "dispatching"
            or original.phase != "dispatching"
            or original.invocation_id != invocation
            or any(
                row[key] != getattr(original, key)
                for key in ("execution_id", "kind", "case_id", "slot_ordinal", "candidate_id")
            )
            or (
                cp.execution_id,
                cp.owner,
                cp.kind,
                cp.case_id,
                cp.slot_ordinal,
                cp.candidate_id,
                cp.execution_ordinal,
                cp.dispatch_started_at,
            )
            != (
                original.execution_id,
                original.owner,
                original.kind,
                original.case_id,
                original.slot_ordinal,
                original.candidate_id,
                original.execution_ordinal,
                original.dispatch_started_at,
            )
        ):
            raise CheckpointConflict("Unmatched dispatch requires recovery")
        if invocation in seen:
            raise CheckpointConflict("Duplicate active dispatch")
        seen.add(invocation)
    if any(
        c.checkpoint.phase == "dispatching" and c.checkpoint.invocation_id not in terminal | seen
        for c in active
    ):
        raise CheckpointConflict("Dispatched claim has no ledger")
    return filtered


async def dispatch_claim(
    db: AsyncSession,
    organization_id: int,
    claim: SlotClaim,
    at: datetime,
) -> SlotClaim:
    from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import record_dispatch

    run, _ = await lock_run(db, claim.run_id, organization_id)
    await active_claims(db, claim.run_id)
    current = await require_claim(db, claim)
    progress = run["progress_json"]
    if (
        progress["status"] != "collecting"
        or progress.get("cancel_requested")
        or progress.get("unresolved_result_unknown_count", 0)
        or progress.get("preflight", {}).get("status") != "passed"
        or at >= current.checkpoint.lease_expires_at
    ):
        raise CheckpointConflict("Run cannot dispatch new work")
    cp = current.checkpoint.mark_dispatching(current.checkpoint.owner, at)
    await record_dispatch(db, claim.run_id, cp)
    await db.execute(update(claims).where(*identity(claim)).values(checkpoint_json=encode(cp)))
    return replace(current, checkpoint=cp)
