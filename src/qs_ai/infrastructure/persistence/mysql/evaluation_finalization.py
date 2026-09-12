"""Finalize and revalidate immutable gate decisions against the original evidence."""

import json
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.finalization import finalize_review
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import evaluate_snapshot
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


def final_record(preview: GatePreview, actor: str, reason: str) -> dict:
    decision = finalize_review(preview.quality, actor, reason)
    record = {
        "schema_version": "qs-ai-evaluation-finalization/v1",
        "run_id": preview.run_id,
        "source_version": preview.version,
        "version": preview.version + 1,
        "release_fingerprint": preview.release_fingerprint,
        **asdict(decision),
        "finalized_at": decision.finalized_at.isoformat(),
        "status": decision.status,
        "gate_result": {
            **asdict(preview.quality),
            "evaluated_at": preview.quality.evaluated_at.isoformat(),
            "gate_passes": dict(preview.gate_passes),
        },
    }
    encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 256 * 1024:
        raise ValueError("Finalization evidence exceeds response bound")
    return json.loads(encoded)


def final_transition(record: dict) -> dict:
    return {
        "from": "awaiting_review",
        "to": record["status"],
        "actor": record["actor"],
        "cause_code": "human_review_finalized",
        "reason": record["reason"],
        "at": record["finalized_at"],
    }


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
    # Set isolation before any reads. Locking reads see the latest committed state;
    # the evidence snapshot is established only after the shared writer lock is held.
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
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
    if checkpoint is None or checkpoint["version"] != expected_version:
        raise CheckpointConflict("Finalization version changed")
    preview = await evaluate_snapshot(
        db, scope, {**run, "version": checkpoint["version"]}, expected_version, at
    )
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
        return ""
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
    preview = await evaluate_snapshot(
        db,
        scope,
        {**run, "version": source_version, "progress_json": awaiting},
        source_version,
        datetime.fromisoformat(record["finalized_at"]),
    )
    expected = final_record(preview, record["actor"], record["reason"])
    if json.dumps(record, sort_keys=True, allow_nan=False) != json.dumps(
        expected, sort_keys=True, allow_nan=False
    ):
        raise ValueError("Final gate differs from original evidence")
    return json.dumps(expected, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
