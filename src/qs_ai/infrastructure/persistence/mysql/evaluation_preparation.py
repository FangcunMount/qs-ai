"""Atomically prepare the next planned execution from persisted terminal evidence."""

import asyncio
import json
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.actions import next_action
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions as evaluation_generation_completions,
)
from qs_ai.infrastructure.persistence.mysql.completion_records import (
    semantic_completions as evaluation_semantic_completions,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_frozen_policies import frozen_policies
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import project_slots
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_runs,
)


async def plan_prepare_execution(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    execution_id: str,
    invocation_id: str,
    at: datetime,
    lease_expires_at: datetime,
    *,
    lock: bool = False,
) -> CheckpointState:
    """Read the original serial action; only the original writer requests locks."""
    checkpoint_query = select(evaluation_checkpoints).where(
        evaluation_checkpoints.c.run_id == str(run_id)
    )
    if lock:
        checkpoint_query = checkpoint_query.with_for_update()
    checkpoint = (await db.execute(checkpoint_query)).mappings().one_or_none()
    if (
        checkpoint is None
        or checkpoint["version"] != expected_version
        or checkpoint["checkpoint_json"] is not None
    ):
        raise CheckpointConflict("Run version changed or execution is active")
    run_query = select(evaluation_runs).where(
        evaluation_runs.c.run_id == str(run_id),
        evaluation_runs.c.organization_id == organization_id,
    )
    if lock:
        run_query = run_query.with_for_update()
    run = (await db.execute(run_query)).mappings().one_or_none()
    if run is None or run["progress_json"] is None or run["execution_mode"] != "serial_v1":
        raise CheckpointConflict("Run progress unavailable")
    if run["progress_json"].get("cancel_requested"):
        raise CheckpointConflict("Cancellation requested; no new dispatch")
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    policy, _ = await asyncio.to_thread(frozen_policies, creation)
    if creation["execution_policy_json"] != policy.definition_json:
        raise CheckpointConflict("Unsupported frozen execution policy")
    dispatch_query = select(evaluation_dispatches).where(
        evaluation_dispatches.c.run_id == str(run_id)
    )
    generation_query = evaluation_generation_completions.select().where(
        evaluation_generation_completions.c.run_id == str(run_id)
    )
    semantic_query = evaluation_semantic_completions.select().where(
        evaluation_semantic_completions.c.run_id == str(run_id)
    )
    if lock:
        dispatch_query = dispatch_query.with_for_update()
        generation_query = generation_query.with_for_update()
        semantic_query = semantic_query.with_for_update()
    dispatches = (await db.execute(dispatch_query)).mappings().all()
    completions = (await db.execute(generation_query)).mappings().all()
    semantic = (await db.execute(semantic_query)).mappings().all()
    slots = await asyncio.to_thread(
        project_slots,
        creation["slots"],
        list(completions),
        list(dispatches),
        list(semantic),
        run["progress_json"].get("result_unknown_resolutions", []),
        run["progress_json"].get("semantic_contract_recoveries", []),
        policy=policy,
    )
    preflight = progress.get("preflight", creation["preflight"])
    action = next_action(
        progress["status"],
        preflight["status"],
        preflight["case_id"],
        slots,
        policy,
        unresolved_unknown=progress.get("unresolved_result_unknown_count", 0),
    )
    if action.kind not in ("generation", "semantic") or action.resume:
        raise CheckpointConflict("Next action is not a new model execution")
    if at < datetime.fromisoformat(creation["audit"]["created_at"]):
        raise ValueError("Preparation cannot precede Run creation")
    if any(
        at < datetime.fromisoformat(r["resolved_at"])
        for r in progress.get("semantic_contract_recoveries", [])
    ):
        raise ValueError("Preparation cannot precede recovery authorization")
    prepared = ExecutionCheckpoint(
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
        lease_expires_at,
    )
    return CheckpointState(run_id, expected_version + 1, prepared)


async def prepare_execution(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    execution_id: str,
    invocation_id: str,
    at: datetime,
    lease_expires_at: datetime,
) -> CheckpointState:
    """Original lock/read order and one checkpoint CAS; caller retains its dispatch/root."""
    state = await plan_prepare_execution(
        db,
        run_id,
        expected_version,
        organization_id,
        owner,
        execution_id,
        invocation_id,
        at,
        lease_expires_at,
        lock=True,
    )
    await save_checkpoint(db, state, expected_version)
    return state
