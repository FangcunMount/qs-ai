"""Reconcile an exact expired checkpoint in the caller's authorized transaction."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.interpretation.route_assets import RouteReader
from qs_ai.application.interpretation.schema_assets import SchemaReader
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions as evaluation_generation_completions,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import complete_generation
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import complete_semantic
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_runs,
)


@dataclass(frozen=True)
class ExpiredRecoverySource:
    checkpoint: ExecutionCheckpoint = field(repr=False)
    run: dict = field(repr=False)


def interrupted_completion(
    cp: ExecutionCheckpoint, at: datetime, *, candidate_fingerprint: str = ""
) -> GenerationCompletion | SemanticCompletion:
    """Original interrupted outcome; never a provider response or invocation permit."""
    if cp.phase != "dispatching" or cp.dispatch_started_at is None:
        raise ValueError("Original dispatched checkpoint required")
    failure = ClassifiedFailure(
        "generation_execution" if cp.kind == "generation" else "semantic_evaluation",
        "result_unknown",
        "execution_interrupted",
        False,
        True,
        "manual_acknowledgement",
        "发送中断，无法确认供应商执行结果",
        (cp.execution_id, cp.invocation_id),
    )
    if cp.kind == "generation":
        return GenerationCompletion(
            cp.execution_id,
            cp.case_id,
            cp.slot_ordinal,
            cp.execution_ordinal,
            cp.invocation_id,
            "result_unknown",
            cp.dispatch_started_at,
            at,
            1,
            failure=failure,
        )
    return SemanticCompletion(
        cp.execution_id,
        cp.candidate_id,
        candidate_fingerprint,
        cp.execution_ordinal,
        cp.invocation_id,
        "result_unknown",
        cp.dispatch_started_at,
        at,
        1,
        failure=failure,
    )


def recovery_progress(
    progress: dict,
    cp: ExecutionCheckpoint,
    expected_version: int,
    observed_expiry: datetime,
    at: datetime,
    actor: str,
) -> dict:
    event = dict(
        actor=actor,
        cause_code=(
            "expired_preparation_released"
            if cp.phase == "prepared"
            else "expired_dispatch_result_unknown"
        ),
        invocation_id=cp.invocation_id,
        execution_id=cp.execution_id,
        previous_owner=cp.owner,
        observed_expiry=observed_expiry.isoformat(),
        at=at.isoformat(),
        from_version=expected_version,
        to_version=expected_version + 1,
    )
    return {**progress, "recoveries": [*progress.get("recoveries", []), event]}


def _read_statement(statement: Select[Any], lock: bool) -> Select[Any]:
    return statement.with_for_update() if lock else statement


async def read_expired_recovery_source(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    invocation_id: str,
    observed_expiry: datetime,
    at: datetime,
    actor: str,
    *,
    lock: bool,
) -> ExpiredRecoverySource:
    """Original expiry source; observation grants no rollback or model permission."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", actor):
        raise ValueError("Recovery actor required")
    if any(t.tzinfo is None or t.utcoffset() is None for t in (observed_expiry, at)):
        raise ValueError("Recovery times require time zones")
    row = (
        (
            await db.execute(
                _read_statement(
                    select(evaluation_checkpoints).where(
                        evaluation_checkpoints.c.run_id == str(run_id)
                    ),
                    lock,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["version"] != expected_version:
        raise CheckpointConflict("Recovery version changed")
    cp = decode(row["checkpoint_json"])
    if (
        cp is None
        or cp.invocation_id != invocation_id
        or cp.lease_expires_at != observed_expiry
        or at < observed_expiry
    ):
        raise CheckpointConflict("Exact expired checkpoint required")
    run = (
        (
            await db.execute(
                _read_statement(
                    select(evaluation_runs).where(
                        evaluation_runs.c.run_id == str(run_id),
                        evaluation_runs.c.organization_id == organization_id,
                    ),
                    lock,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        run is None
        or run["progress_json"] is None
        or run["progress_json"]["status"] != "collecting"
    ):
        raise CheckpointConflict("Collecting Run in organization required")
    return ExpiredRecoverySource(cp, dict(run))


async def recover_expired(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    invocation_id: str,
    observed_expiry: datetime,
    at: datetime,
    actor: str,
    routes: RouteReader,
    schemas: SchemaReader,
) -> CheckpointState:
    """No network calls or commit. Caller authorizes actor; unknown is never a resend permit."""
    source = await read_expired_recovery_source(
        db,
        run_id,
        expected_version,
        organization_id,
        invocation_id,
        observed_expiry,
        at,
        actor,
        lock=True,
    )
    cp = source.checkpoint
    if cp.phase == "prepared":
        # A corrupted prepared marker must never erase a committed dispatch.
        dispatch = (
            await db.execute(
                select(evaluation_dispatches.c.invocation_id).where(
                    evaluation_dispatches.c.run_id == str(run_id),
                    evaluation_dispatches.c.execution_id == cp.execution_id,
                )
            )
        ).first()
        if dispatch is not None:
            raise ValueError("Prepared checkpoint has dispatch evidence")
        state = CheckpointState(run_id, expected_version + 1, None)
        await save_checkpoint(db, state, expected_version)
    else:
        if cp.kind == "generation":
            completion = interrupted_completion(cp, at)
            assert isinstance(completion, GenerationCompletion)
            state = await complete_generation(
                db,
                run_id,
                expected_version,
                organization_id,
                cp.owner,
                completion,
                routes,
                schemas,
            )
        else:
            generated = (
                (
                    await db.execute(
                        evaluation_generation_completions.select().where(
                            evaluation_generation_completions.c.run_id == str(run_id),
                            evaluation_generation_completions.c.candidate_id == cp.candidate_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
            fingerprint = decode_completion(generated).normalized_fingerprint
            completion = interrupted_completion(cp, at, candidate_fingerprint=fingerprint)
            assert isinstance(completion, SemanticCompletion)
            state = await complete_semantic(
                db,
                run_id,
                expected_version,
                organization_id,
                cp.owner,
                completion,
                routes,
            )
    # Read the terminal acceptor's new progress, then append audit in the same transaction.
    progress = (
        await db.execute(
            select(evaluation_runs.c.progress_json).where(evaluation_runs.c.run_id == str(run_id))
        )
    ).scalar_one()
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(
            progress_json=recovery_progress(
                progress, cp, expected_version, observed_expiry, at, actor
            )
        )
    )
    return state
