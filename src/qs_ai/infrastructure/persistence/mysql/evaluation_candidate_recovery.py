"""Recover expired candidate ownership from evidence; never calls a model."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import emit
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    read_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    active_claims,
    lock_run,
    release_claim,
    require_claim,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import finish_step, prepare_step
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_runs,
    evaluation_slot_claims,
)


@dataclass(frozen=True)
class CandidateRecoverySource:
    version: int
    run: dict = field(repr=False)
    claim: SlotClaim = field(repr=False)
    evidence: EvaluationResponse | None = field(repr=False)


CandidateRecoveryDispatch = Callable[[int, SlotClaim, datetime], Awaitable[bool]]


async def read_candidate_recovery_source(
    db: AsyncSession, organization_id: int, claim: SlotClaim, at: datetime, *, lock: bool
) -> CandidateRecoverySource | None:
    """Original expired ownership and first evidence; snapshot grants no commit or model right."""
    from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
        completed_claim_state,
    )

    if await completed_claim_state(db, organization_id, claim, lock=lock) is not None:
        return None
    run, version = await lock_run(db, claim.run_id, organization_id, lock=lock)
    await active_claims(db, claim.run_id, lock=lock)
    current = await require_claim(db, claim, lock=lock)
    cp = current.checkpoint
    if cp.lease_expires_at != claim.checkpoint.lease_expires_at or at < cp.lease_expires_at:
        raise CheckpointConflict("Exact expired candidate required")
    if cp.phase == "prepared":
        ledger = (
            await db.execute(
                select(evaluation_dispatches).where(
                    evaluation_dispatches.c.run_id == str(claim.run_id),
                    evaluation_dispatches.c.execution_id == cp.execution_id,
                )
            )
        ).first()
        if ledger is not None:
            raise CheckpointConflict("Prepared candidate has dispatch evidence")
        evidence = None
    else:
        evidence = await read_response(db, current)
    return CandidateRecoverySource(version, run, current, evidence)


async def recover_candidate(
    transactions: Transactions,
    organization_id: int,
    claim: SlotClaim,
    at: datetime,
    routes: RouteAssets,
    schemas: SchemaAssets,
) -> None:
    async with transactions.open() as db:
        source = await read_candidate_recovery_source(db, organization_id, claim, at, lock=True)
        if source is None:
            return
        version, current, evidence = source.version, source.claim, source.evidence
        cp = current.checkpoint
        if cp.phase == "prepared":
            await release_claim(db, current)
            await save_checkpoint(db, CheckpointState(claim.run_id, version + 1, None), version)
            await db.commit()
            emit(
                "evaluation.candidate_recovered",
                "evaluation",
                run_id=str(claim.run_id),
                invocation_id=cp.invocation_id,
                stage=cp.kind,
                status="prepared_released",
            )
            return
    prepared = await prepare_step(
        transactions,
        claim.run_id,
        version,
        organization_id,
        cp.owner,
        routes,
        schemas,
        resume_claim=current,
        clock=lambda: at,
    )
    failure = (
        ClassifiedFailure(
            "generation_execution" if cp.kind == "generation" else "semantic_evaluation",
            "result_unknown",
            "execution_interrupted",
            False,
            True,
            "manual_acknowledgement",
            "发送中断，无法确认供应商执行结果",
            (cp.execution_id, cp.invocation_id),
        )
        if evidence is None
        else evidence.failure
    )
    await finish_step(
        transactions,
        claim.run_id,
        organization_id,
        cp.owner,
        routes,
        schemas,
        prepared,
        evidence.response if evidence else None,
        failure,
        clock=lambda: evidence.finished_at if evidence else at,
        recovery_at=at,
    )

    emit(
        "evaluation.candidate_recovered",
        "evaluation",
        run_id=str(claim.run_id),
        invocation_id=cp.invocation_id,
        stage=cp.kind,
        status="receipt_recovered" if evidence else "result_unknown",
    )


async def recover_next_candidate(
    transactions: Transactions,
    routes: RouteAssets,
    schemas: SchemaAssets,
    at: datetime,
    cursor: RecoveryCursor,
    *,
    dispatch: CandidateRecoveryDispatch | None = None,
) -> bool:
    claims = evaluation_slot_claims
    async with transactions.open() as db:
        rows = (
            (
                await db.execute(
                    select(claims, evaluation_runs.c.organization_id)
                    .join(evaluation_runs, evaluation_runs.c.run_id == claims.c.run_id)
                    .where(
                        tuple_(claims.c.run_id, claims.c.case_id, claims.c.slot_ordinal)
                        > cursor.after_candidate
                    )
                    .order_by(claims.c.run_id, claims.c.case_id, claims.c.slot_ordinal)
                    .limit(64)
                )
            )
            .mappings()
            .all()
        )
    for row in rows:
        cursor.after_candidate = (row["run_id"], row["case_id"], row["slot_ordinal"])
        cp = decode(row["checkpoint_json"])
        if cp is None:
            raise CheckpointConflict("Missing candidate checkpoint")
        if cp.lease_expires_at > at:
            continue
        try:
            observed = SlotClaim(UUID(row["run_id"]), row["version"], cp)
            if dispatch is not None:
                if not await dispatch(row["organization_id"], observed, at):
                    continue
            else:
                await recover_candidate(
                    transactions,
                    row["organization_id"],
                    observed,
                    at,
                    routes,
                    schemas,
                )
        except CheckpointConflict:
            continue
        return True
    if len(rows) < 64:
        cursor.after_candidate = ("", "", 0)
    return False
