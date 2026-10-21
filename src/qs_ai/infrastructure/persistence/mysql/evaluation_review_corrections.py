"""CAS correction of one's own decision, with durable command replay and no model calls."""

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.domain.evaluation.review_correction import ReviewCorrection, validate_corrections
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import (
    lock_run_in_transaction,
    save_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import load_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_review_codec import (
    decode_corrections,
    decode_reviews,
    encode_correction,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints


async def correct_review(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    command_id: str,
    previous_review_fingerprint: str,
    candidate_output_fingerprint: str,
    value: CandidateHumanReview,
) -> None:
    if type(expected_version) is not int or expected_version < 1 or value.reviewer != scope.actor:
        raise ValueError("Trusted original reviewer and explicit version required")
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    # Always lock checkpoint first. Replay checks precede CAS, so a lost response
    # can be recovered after subsequent reviews/finalization without a new write.
    version = (
        await db.execute(
            select(evaluation_checkpoints.c.version)
            .where(evaluation_checkpoints.c.run_id == str(scope.run_id))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if version is None:
        from qs_ai.application.interpretation.ports import NotFound

        raise NotFound("Evaluation unavailable in organization")
    run = await lock_run_in_transaction(db, scope, version)
    updated = await plan_review_correction(
        db,
        scope,
        expected_version,
        command_id,
        previous_review_fingerprint,
        candidate_output_fingerprint,
        value,
        run=run,
    )
    if updated is not None:
        await save_progress(db, scope, version, updated)


async def plan_review_correction(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    command_id: str,
    previous_review_fingerprint: str,
    candidate_output_fingerprint: str,
    value: CandidateHumanReview,
    *,
    run: Mapping[Any, Any],
) -> dict | None:
    """Only read original evidence; None is an exact historical business replay.

    Caller supplies a scoped RR snapshot or the locked original Run. Replay stays
    before version/status CAS and authorizes no new event or transaction witness.
    """
    if type(expected_version) is not int or expected_version < 1 or value.reviewer != scope.actor:
        raise ValueError("Trusted original reviewer and explicit version required")
    version = run["version"]
    progress = run["progress_json"]
    if correction_replayed(
        scope,
        expected_version,
        command_id,
        previous_review_fingerprint,
        candidate_output_fingerprint,
        value,
        progress,
    ):
        return None
    if version != expected_version or progress["status"] != "awaiting_review":
        raise CheckpointConflict("Review correction requires current awaiting-review version")
    snapshot = await load_snapshot(db, scope, run, version, value.reviewed_at)
    originals = decode_reviews(progress.get("human_reviews", []))
    corrections = decode_corrections(progress)
    previous = [
        r for r in snapshot.reviews if (r.candidate_id, r.role) == (value.candidate_id, value.role)
    ]
    if len(previous) != 1 or previous[0].reviewer != scope.actor:
        raise ValueError("Only the original reviewer may correct their own decision")
    entry = ReviewCorrection(
        command_id,
        version,
        version + 1,
        previous_review_fingerprint,
        candidate_output_fingerprint,
        previous[0],
        value,
    )
    validate_corrections(
        originals,
        (*corrections, entry),
        snapshot.candidates,
        snapshot.closed_at,
        version + 1,
        value.reviewed_at,
        "v2",
    )
    return {
        **progress,
        "review_corrections": [
            *(encode_correction(c) for c in corrections),
            encode_correction(entry),
        ],
    }


def correction_replayed(
    scope: ManagementScope,
    expected_version: int,
    command_id: str,
    previous_review_fingerprint: str,
    candidate_output_fingerprint: str,
    value: CandidateHumanReview,
    progress: dict,
) -> bool:
    """Exact original business receipt only, independent of today's version CAS."""
    archives = [
        progress,
        *(
            {"review_corrections": r.get("previous_review_corrections", [])}
            for r in progress.get("review_reopenings", [])
        ),
    ]
    for archive in archives:
        for prior in decode_corrections(archive):
            if prior.command_id != command_id:
                continue
            if (
                prior.source_version != expected_version
                or prior.previous_review_fingerprint != previous_review_fingerprint
                or prior.candidate_output_fingerprint != candidate_output_fingerprint
                or (
                    prior.review.candidate_id,
                    prior.review.role,
                    prior.review.reviewer,
                    prior.review.decision,
                    prior.review.reason,
                )
                != (value.candidate_id, value.role, scope.actor, value.decision, value.reason)
            ):
                raise CheckpointConflict("Correction command ID reused with different intent")
            return True
    return False
