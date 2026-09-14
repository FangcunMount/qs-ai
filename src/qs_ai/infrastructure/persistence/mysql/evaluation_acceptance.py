"""Adopt a versioned acceptance rule on an unfinalized Run; never rewrite execution evidence."""

import json
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.acceptance import acceptance_version, rule_document
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import lock_run, save_progress
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import evaluate_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import has_review_rounds


async def adopt_acceptance_rule(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    expected_release_fingerprint: str,
    actor: str,
    reason: str,
    at: datetime,
    *,
    confirm: bool,
) -> dict:
    """Caller is a trusted operator acting on authorized QS scope and owns commit/rollback."""
    if confirm is not True:
        raise ValueError("Explicit acceptance rule change confirmation required")
    run = await lock_run(db, scope, expected_version)
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    if (
        not progress
        or progress["status"] != "awaiting_review"
        or creation.get("acceptance_rule") is not None
        or progress.get("acceptance_rule_adoption") is not None
        or progress.get("gate_result") is not None
        or progress.get("finalized_at") is not None
        or has_review_rounds(progress)
        or creation["release_fingerprint"] != expected_release_fingerprint
    ):
        raise ValueError("Only an unfinalized legacy Run may adopt this acceptance rule once")
    if any(
        at < datetime.fromisoformat(r["reviewed_at"]) for r in progress.get("human_reviews", [])
    ):
        raise ValueError("Adoption cannot precede existing human reviews")
    before = await evaluate_snapshot(db, scope, run, expected_version, at)
    adoption = {
        "rule": rule_document(),
        "actor": actor,
        "reason": reason,
        "adopted_at": at.isoformat(),
        "source_version": expected_version,
        "version": expected_version + 1,
        "release_fingerprint": expected_release_fingerprint,
    }
    updated = {**progress, "acceptance_rule_adoption": adoption}
    acceptance_version(creation, updated, expected_version + 1, at)
    await save_progress(db, scope, expected_version, updated)
    after = await evaluate_snapshot(
        db,
        scope,
        {**run, "version": expected_version + 1, "progress_json": updated},
        expected_version + 1,
        at,
    )
    if any(
        dict(before.gate_passes)[g] != dict(after.gate_passes)[g] for g in ("G1", "G2", "G4", "G5")
    ):
        raise ValueError(
            "Acceptance change must not change identity, evidence, quality or review gates"
        )
    return {
        "run_id": str(scope.run_id),
        "adoption": adoption,
        "before_gates": dict(before.gate_passes),
        "after_gates": dict(after.gate_passes),
        "metrics": [vars(m) for m in after.quality.metrics],
    }
