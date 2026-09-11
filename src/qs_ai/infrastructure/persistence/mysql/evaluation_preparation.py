"""Atomically prepare the first planned execution without guessing terminal evidence."""

import json
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.actions import SlotProgress, next_action
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


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
    checkpoint = (
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
    if (
        checkpoint is None
        or checkpoint["version"] != expected_version
        or checkpoint["checkpoint_json"] is not None
    ):
        raise CheckpointConflict("Run version changed or execution is active")
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
    if run is None or run["progress_json"] is None:
        raise CheckpointConflict("Run progress unavailable")
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    policy = load_execution_policy()
    if creation["execution_policy_json"] != policy.definition_json:
        raise CheckpointConflict("Unsupported frozen execution policy")
    # A dispatch ledger is not terminal evidence. Never turn it into an empty slot
    # or guess that a call failed while receipt/candidate acceptance is not wired.
    dispatched = (
        await db.execute(
            select(evaluation_dispatches.c.invocation_id)
            .where(evaluation_dispatches.c.run_id == str(run_id))
            .limit(1)
            .with_for_update()
        )
    ).first()
    if dispatched is not None:
        raise CheckpointConflict("Terminal execution evidence projection required")
    slots = tuple(SlotProgress(slot["case_id"], slot["ordinal"]) for slot in creation["slots"])
    preflight = progress.get("preflight", creation["preflight"])
    action = next_action(
        progress["status"], preflight["status"], preflight["case_id"], slots, policy
    )
    if action.kind != "generation" or action.resume:
        raise CheckpointConflict("Next action is not a new generation")
    if at < datetime.fromisoformat(creation["audit"]["created_at"]):
        raise ValueError("Preparation cannot precede Run creation")
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
    state = CheckpointState(run_id, expected_version + 1, prepared)
    await save_checkpoint(db, state, expected_version)
    return state
