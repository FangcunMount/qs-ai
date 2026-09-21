"""Accept an already authorized manual decision under the shared Run version."""

import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.resolution import (
    ResultUnknownResolution,
    resolve_unknown,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution_evidence import (
    load_resolution_evidence,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_runs,
)


async def accept_resolution(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    value: ResultUnknownResolution,
    *,
    confirm: bool,
) -> CheckpointState:
    """Caller supplies authorized organization/actor and owns commit; never edits executions."""
    if confirm is not True:
        raise ValueError("Explicit confirmation required")
    cp = (
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
    if cp is None or cp["version"] != expected_version or cp["checkpoint_json"] is not None:
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
        raise CheckpointConflict("Run unavailable in organization")
    from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import active_claims

    if run["execution_mode"] == "candidate_v2" and await active_claims(db, run_id):
        raise CheckpointConflict("Drain candidate executions before resolving unknown calls")
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    evidence = await load_resolution_evidence(db, run_id, creation, progress)
    result = resolve_unknown(
        progress["status"], evidence.unknowns, evidence.resolutions, value, evidence.policy
    )
    if progress.get("cancel_requested") and result.status == "canceled" and result.unresolved_count:
        raise CheckpointConflict("Resolve remaining unknown calls before cancellation completes")
    if value.resolved_at < datetime.fromisoformat(creation["audit"]["created_at"]):
        raise ValueError("Resolution precedes Run creation")
    updated = {
        **progress,
        "status": result.status,
        "unresolved_result_unknown_count": result.unresolved_count,
        "result_unknown_resolutions": [
            {**asdict(r), "resolved_at": r.resolved_at.isoformat()} for r in result.resolutions
        ],
    }
    if result.status != progress["status"]:
        updated["transitions"] = [
            *progress["transitions"],
            {
                "from": progress["status"],
                "to": result.status,
                "cause_code": "result_unknown_run_canceled"
                if result.status == "canceled"
                else "manual_recovery_approved",
                "actor": value.actor,
                "at": value.resolved_at.isoformat(),
                "evidence_refs": [value.execution_id],
            },
        ]
    if result.status == "canceled":
        updated["canceled_at"] = value.resolved_at.isoformat()
    state = CheckpointState(run_id, expected_version + 1, None)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(progress_json=updated)
    )
    return state
