"""Preserve a rejected round before reopening its eligible signatures under Run CAS."""

from dataclasses import asdict
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.reopening import reopen_review
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import (
    lock_run,
    save_progress,
    verified_final_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import (
    canonical,
    opening_record,
    opening_transition,
)


async def reopen(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    reason: str,
    at: datetime,
    *,
    confirm: bool,
) -> None:
    if confirm is not True or type(expected_version) is not int or expected_version < 1:
        raise ValueError("Explicit version and confirmation required")
    run = await lock_run(db, scope, expected_version)
    if not run["progress_json"] or run["progress_json"]["status"] != "rejected":
        raise CheckpointConflict("Only a finalized rejection can be reopened")
    snapshot = await verified_final_snapshot(db, scope, run)
    progress = run["progress_json"]
    history = progress.get("review_reopenings", [])
    plan = reopen_review(
        snapshot.candidates,
        snapshot.reviews,
        snapshot.preview.quality,
        snapshot.closed_at,
        scope.actor,
        reason,
        at,
        status=progress["status"],
        gate_policy_version="v2",
        reopening_count=len(history),
    )
    entry = opening_record(plan, progress["gate_result"], len(progress["transitions"]))
    history = [*history, entry]
    if len(canonical(history).encode()) > 2 * 1024 * 1024:
        raise ValueError("Review history exceeds response bound")
    await save_progress(
        db,
        scope,
        expected_version,
        {
            **progress,
            "status": "awaiting_review",
            "gate_result": None,
            "finalized_at": None,
            "human_reviews": [
                {**asdict(r), "reviewed_at": r.reviewed_at.isoformat()}
                for r in plan.retained_reviews
            ],
            "review_reopenings": history,
            "transitions": [*progress["transitions"], opening_transition(entry)],
        },
    )
