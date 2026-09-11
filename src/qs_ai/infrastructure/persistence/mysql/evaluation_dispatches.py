"""Dispatch reservation inside a caller-owned transaction; no provider call occurs here."""

import hashlib
import json
from datetime import datetime
from uuid import UUID

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import (
    decode,
    encode,
    save_checkpoint,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints as checkpoints,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches as dispatches,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_run_policies as policies,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


async def freeze_policy(db: AsyncSession, run_id: UUID, policy: ExecutionPolicy) -> None:
    # No upsert: changing a Run policy after reservation must never reset its budget.
    if (
        "sha256:" + hashlib.sha256(policy.definition_json.encode()).hexdigest()
        != policy.fingerprint
    ):
        raise ValueError("Policy fingerprint mismatch")
    if policy != load_execution_policy():
        raise ValueError("Unregistered evaluation policy")
    await db.execute(
        insert(policies).values(
            run_id=str(run_id),
            fingerprint=policy.fingerprint,
            definition_json=policy.definition_json,
        )
    )


async def reserve_dispatch(
    db: AsyncSession, run_id: UUID, expected_version: int, owner: str, at: datetime
) -> CheckpointState:
    # Every reservation for a Run uses the same lock, including different slots/stages.
    row = (
        (
            await db.execute(
                select(checkpoints).where(checkpoints.c.run_id == str(run_id)).with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["version"] != expected_version:
        raise CheckpointConflict("Checkpoint version changed")
    checkpoint = decode(row["checkpoint_json"])
    if checkpoint is None:
        raise CheckpointConflict("Prepared checkpoint required")
    dispatched = checkpoint.mark_dispatching(owner, at)
    policy = (
        await db.execute(select(policies.c.definition_json).where(policies.c.run_id == str(run_id)))
    ).scalar_one()
    policy = json.loads(policy)
    stage = checkpoint.kind
    budget = policy["generation_budget" if stage == "generation" else "semantic_budget"]
    # Locking reads see the latest committed ledger even if the caller already read in RR.
    entries = (
        (
            await db.execute(
                select(dispatches)
                .where(dispatches.c.run_id == str(run_id), dispatches.c.kind == stage)
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    total = len(entries)
    used = sum(
        item["case_id"] == checkpoint.case_id
        and item["slot_ordinal"] == checkpoint.slot_ordinal
        and (stage == "generation" or item["candidate_id"] == checkpoint.candidate_id)
        for item in entries
    )
    limit = budget[
        "max_executions_per_slot" if stage == "generation" else "max_executions_per_candidate"
    ]
    if used >= limit or total >= budget["max_executions_per_run"]:
        raise CheckpointConflict("Evaluation execution budget exhausted")
    state = CheckpointState(run_id, expected_version + 1, dispatched)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        insert(dispatches).values(
            run_id=str(run_id),
            invocation_id=checkpoint.invocation_id,
            kind=stage,
            case_id=checkpoint.case_id,
            slot_ordinal=checkpoint.slot_ordinal,
            candidate_id=checkpoint.candidate_id,
            checkpoint_json=encode(dispatched),
        )
    )
    return state
