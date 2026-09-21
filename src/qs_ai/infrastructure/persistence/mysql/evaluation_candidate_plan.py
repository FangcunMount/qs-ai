"""Plan candidate work from locked durable evidence, including active dispatches."""

import asyncio
import json
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.parallel_plan import ParallelPlan, plan_parallel
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_frozen_policies import frozen_policies
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import project_slots
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    active_claims,
    create_claim,
    lock_run,
    terminal_dispatches,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_generation_completions,
    evaluation_semantic_completions,
)


async def candidate_plan(
    db: AsyncSession,
    run_id: UUID,
    run: dict,
    active: tuple[SlotClaim, ...],
    limit: int,
) -> ParallelPlan:
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    policy, _ = await asyncio.to_thread(frozen_policies, creation)
    records = []
    for table in (
        evaluation_dispatches,
        evaluation_generation_completions,
        evaluation_semantic_completions,
    ):
        records.append(
            list(
                (
                    await db.execute(
                        select(table).where(table.c.run_id == str(run_id)).with_for_update()
                    )
                )
                .mappings()
                .all()
            )
        )
    dispatches, generations, semantics = records
    terminal = terminal_dispatches(dispatches, generations + semantics, active)
    slots = await asyncio.to_thread(
        project_slots,
        creation["slots"],
        generations,
        terminal,
        semantics,
        progress.get("result_unknown_resolutions", []),
        progress.get("semantic_contract_recoveries", []),
        policy=policy,
    )
    preflight = progress.get("preflight", creation["preflight"])
    return plan_parallel(
        progress["status"],
        preflight["status"],
        preflight["case_id"],
        slots,
        policy,
        active_slots=frozenset((c.checkpoint.case_id, c.checkpoint.slot_ordinal) for c in active),
        limit=limit,
        unresolved_unknown=progress.get("unresolved_result_unknown_count", 0),
    )


async def prepare_candidate(
    db: AsyncSession,
    run_id: UUID,
    organization_id: int,
    owner: str,
    execution_id: str,
    invocation_id: str,
    at: datetime,
    expires_at: datetime,
    *,
    limit: int,
) -> SlotClaim:
    run, version = await lock_run(db, run_id, organization_id)
    if not run["progress_json"] or run["progress_json"].get("cancel_requested"):
        raise CheckpointConflict("Run is stopping or has no progress")
    active = await active_claims(db, run_id)
    plan = await candidate_plan(db, run_id, run, active, limit)
    if not plan.ready:
        raise CheckpointConflict("No candidate capacity or ready work")
    creation = json.loads(run["definition_json"])
    times = [creation["audit"]["created_at"]] + [
        r["resolved_at"] for r in run["progress_json"].get("semantic_contract_recoveries", [])
    ]
    if any(at < datetime.fromisoformat(value) for value in times):
        raise ValueError("Preparation cannot precede source evidence")
    action = plan.ready[0]
    cp = ExecutionCheckpoint(
        execution_id,
        action.kind,
        action.case_id,
        action.slot_ordinal,
        action.candidate_id,
        action.execution_ordinal,
        owner,
        invocation_id,
        "prepared",
        at,
        expires_at,
    )
    claim = await create_claim(db, run_id, cp, version + 1)
    await save_checkpoint(db, CheckpointState(run_id, version + 1, None), version)
    return claim
