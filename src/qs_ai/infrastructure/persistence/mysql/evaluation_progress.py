"""Initial lifecycle transitions share the checkpoint CAS and caller transaction."""

import json
import re
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.preflight import PreflightEvidence
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def transition_requested(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    target: str,
    actor: str,
    reason: str,
    at: datetime,
) -> CheckpointState:
    """Only requested -> collecting/canceled; does not authorize the supplied actor."""
    if target not in ("collecting", "canceled"):
        raise ValueError("Unsupported initial Run transition")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", actor):
        raise ValueError("Invalid transition actor")
    if not reason.strip() or len(reason.encode()) > 1000 or any(x in reason for x in "<>"):
        raise ValueError("Invalid transition reason")
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Transition time must have a time zone")
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
    if run is None:
        raise CheckpointConflict("Run unavailable in organization")
    creation = json.loads(run["definition_json"])
    progress = run["progress_json"]
    if progress is None:
        if expected_version != 1:
            raise CheckpointConflict("Legacy Run progress needs reconciliation")
        progress = {"status": creation["status"], "transitions": creation["transitions"]}
    if progress["status"] != "requested":
        raise CheckpointConflict("Run is no longer requested")
    if at < datetime.fromisoformat(creation["audit"]["created_at"]):
        raise ValueError("Transition cannot precede creation")
    progress = {
        **progress,
        "status": target,
        "transitions": [
            *progress["transitions"],
            {
                "from": "requested",
                "to": target,
                "actor": actor,
                "reason": reason,
                "cause_code": "evaluation_started"
                if target == "collecting"
                else "evaluation_canceled",
                "at": at.isoformat(),
            },
        ],
    }
    if target == "canceled":
        progress["canceled_at"] = at.isoformat()
    state = CheckpointState(run_id, expected_version + 1, None)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(progress_json=progress)
    )
    return state


async def complete_preflight(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    evidence: "PreflightEvidence",
) -> CheckpointState:
    from dataclasses import asdict

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
    if run is None:
        raise CheckpointConflict("Run unavailable in organization")
    creation = json.loads(run["definition_json"])
    progress = run["progress_json"]
    if progress is None or progress["status"] != "collecting" or "preflight" in progress:
        raise CheckpointConflict("Pending preflight in collecting Run required")
    if evidence.case_id != creation["preflight"][
        "case_id"
    ] or evidence.evaluated_at < datetime.fromisoformat(creation["audit"]["created_at"]):
        raise ValueError("Preflight source or time mismatch")
    value = asdict(evidence)
    value["evaluated_at"] = evidence.evaluated_at.isoformat()
    progress = {**progress, "preflight": value}
    if evidence.status == "failed":
        progress["status"] = "blocked"
        progress["transitions"] = [
            *progress["transitions"],
            {
                "from": "collecting",
                "to": "blocked",
                "cause_code": "preflight_failed",
                "actor": "system:runner",
                "at": evidence.evaluated_at.isoformat(),
                "evidence_refs": [evidence.case_id],
            },
        ]
    state = CheckpointState(run_id, expected_version + 1, None)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(progress_json=progress)
    )
    return state


async def execute_preflight(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    at: datetime,
) -> CheckpointState:
    """Compute registered evidence, then accept it under the shared Run version."""
    from qs_ai.domain.evaluation.identity import FrozenContractRef
    from qs_ai.infrastructure.qs_server.preflight import run_preflight

    raw = (
        await db.execute(
            select(evaluation_runs.c.definition_json).where(
                evaluation_runs.c.run_id == str(run_id),
                evaluation_runs.c.organization_id == organization_id,
            )
        )
    ).scalar_one_or_none()
    if raw is None:
        raise CheckpointConflict("Run unavailable in organization")
    creation = json.loads(raw)
    from qs_ai.infrastructure.persistence.mysql.evaluation_assets import stored_run_suite

    suite = await stored_run_suite(db, creation)
    evidence = run_preflight(
        FrozenContractRef(**creation["release"]["suite"]), at, frozen_suite=suite
    )
    return await complete_preflight(db, run_id, expected_version, organization_id, evidence)
