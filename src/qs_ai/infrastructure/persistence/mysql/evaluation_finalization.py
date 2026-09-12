"""Finalize and revalidate immutable gate decisions against the original evidence."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.finalization import final_record, final_transition
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import (
    GateSnapshot,
    evaluate_snapshot,
    load_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import has_review_rounds
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def finalize(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    expected_passed: bool,
    reason: str,
    at: datetime,
    *,
    confirm: bool,
) -> None:
    """Caller supplies trusted scope/time and owns commit; never invoke a model."""
    if (
        confirm is not True
        or type(expected_version) is not int
        or expected_version < 1
        or type(expected_passed) is not bool
    ):
        raise ValueError("Explicit version, expected outcome and confirmation required")
    run = await lock_run(db, scope, expected_version)
    preview = await evaluate_snapshot(db, scope, run, expected_version, at)
    record = final_record(preview, scope.actor, reason)
    if record["passed"] != expected_passed:
        raise CheckpointConflict("Gate outcome changed; refresh preview")
    progress = run["progress_json"]
    updated = {
        **progress,
        "status": record["status"],
        "finalized_at": record["finalized_at"],
        "gate_result": record,
        "transitions": [*progress["transitions"], final_transition(record)],
    }
    await save_progress(db, scope, expected_version, updated)


async def lock_run(db: AsyncSession, scope: ManagementScope, expected_version: int) -> dict:
    # Set isolation before any reads. Locking reads see the latest committed state;
    # the evidence snapshot is established only after the shared writer lock is held.
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    return await lock_run_in_transaction(db, scope, expected_version)


async def lock_run_in_transaction(
    db: AsyncSession, scope: ManagementScope, expected_version: int
) -> dict:
    """Caller configures REPEATABLE READ before any query and owns this transaction."""
    checkpoint = (
        (
            await db.execute(
                select(evaluation_checkpoints)
                .where(evaluation_checkpoints.c.run_id == str(scope.run_id))
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
                    evaluation_runs.c.run_id == str(scope.run_id),
                    evaluation_runs.c.organization_id == scope.organization_id,
                )
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    # Scope is checked before disclosing a version or a terminal decision.
    if run is None:
        raise NotFound("Evaluation unavailable in organization")
    if (
        checkpoint is None
        or checkpoint["version"] != expected_version
        or checkpoint["checkpoint_json"] is not None
    ):
        raise CheckpointConflict("Finalization version changed")
    return {**run, "version": checkpoint["version"]}


async def save_progress(
    db: AsyncSession, scope: ManagementScope, expected_version: int, updated: dict
) -> None:
    await save_checkpoint(
        db, CheckpointState(scope.run_id, expected_version + 1, None), expected_version
    )
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(scope.run_id))
        .values(progress_json=updated)
    )


async def read_finalization(
    db: AsyncSession, scope: ManagementScope, run: Mapping[str, Any]
) -> str:
    """A stored status or JSON gate alone is not sufficient evidence of approval."""
    progress = run["progress_json"]
    record = progress.get("gate_result")
    if record is None and progress["status"] not in ("approved", "rejected"):
        if progress.get("finalized_at"):
            raise ValueError("Finalization audit without a gate decision")
        if has_review_rounds(progress):
            await load_snapshot(db, scope, run, run["version"], datetime.now(UTC))
        return ""
    await verified_final_snapshot(db, scope, run)
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


async def verified_final_snapshot(
    db: AsyncSession, scope: ManagementScope, run: Mapping[str, Any]
) -> GateSnapshot:
    progress = run["progress_json"]
    record = progress.get("gate_result")
    if not isinstance(record, dict) or progress["status"] not in ("approved", "rejected"):
        raise ValueError("Final status and gate decision disagree")
    source_version = record["source_version"]
    if (
        type(source_version) is not int
        or source_version < 1
        or run["version"] != source_version + 1
        or progress.get("finalized_at") != record["finalized_at"]
        or progress["status"] != record["status"]
        or not progress["transitions"]
        or progress["transitions"][-1] != final_transition(record)
    ):
        raise ValueError("Finalized Run audit requires reconciliation")
    awaiting = {
        **progress,
        "status": "awaiting_review",
        "gate_result": None,
        "finalized_at": None,
        "transitions": progress["transitions"][:-1],
    }
    snapshot = await load_snapshot(
        db,
        scope,
        {**run, "version": source_version, "progress_json": awaiting},
        source_version,
        datetime.fromisoformat(record["finalized_at"]),
    )
    expected = final_record(snapshot.preview, record["actor"], record["reason"])
    if json.dumps(record, sort_keys=True, allow_nan=False) != json.dumps(
        expected, sort_keys=True, allow_nan=False
    ):
        raise ValueError("Final gate differs from original evidence")
    return snapshot
