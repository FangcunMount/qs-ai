"""Accept authenticated candidate review batches under the shared Run CAS lock."""

import json
from dataclasses import asdict
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.actions import next_action
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.review import (
    CandidateHumanReview,
    ReviewCandidate,
    add_human_reviews,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_gates import evaluate_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import (
    decode_completion,
    decode_semantic_completion,
    project_slots,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_review_codec import (
    decode_reviews as decode_reviews,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import has_review_rounds
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_generation_completions,
    evaluation_runs,
    evaluation_semantic_completions,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)


async def accept_reviews(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    values: tuple[CandidateHumanReview, ...],
) -> CheckpointState:
    """Caller authorizes the review role, supplies server times and owns the commit."""
    if (
        type(expected_version) is not int
        or expected_version < 1
        or not isinstance(values, tuple)
        or not 1 <= len(values) <= 35
        or any(v.reviewer != scope.actor for v in values)
        or len({v.role for v in values}) != 1
    ):
        raise ValueError("Trusted reviewer, single role and Run version required")
    cp = (
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
    if cp is None or cp["version"] != expected_version or cp["checkpoint_json"] is not None:
        raise CheckpointConflict("Run version changed or execution is active")
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
    if run is None or run["progress_json"] is None:
        raise CheckpointConflict("Run unavailable in organization")
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    policy, gate = load_execution_policy(), load_gate_policy()
    if (creation["execution_policy_json"], creation["gate_policy_json"]) != (
        policy.definition_json,
        gate.definition_json,
    ):
        raise ValueError("Frozen review policies unavailable")
    if progress["status"] != "awaiting_review":
        raise CheckpointConflict("Run is not awaiting review")
    if has_review_rounds(progress):
        await evaluate_snapshot(
            db,
            scope,
            {**run, "version": cp["version"]},
            expected_version,
            max(v.reviewed_at for v in values),
        )
        latest = progress["review_reopenings"][-1]
        if any(
            v.candidate_id in latest["candidate_ids"]
            and v.reviewed_at < datetime.fromisoformat(latest["reopened_at"])
            for v in values
        ):
            raise ValueError("New signature predates reopening")
    generations = list(
        (
            await db.execute(
                select(evaluation_generation_completions).where(
                    evaluation_generation_completions.c.run_id == str(scope.run_id)
                )
            )
        )
        .mappings()
        .all()
    )
    semantics = list(
        (
            await db.execute(
                select(evaluation_semantic_completions).where(
                    evaluation_semantic_completions.c.run_id == str(scope.run_id)
                )
            )
        )
        .mappings()
        .all()
    )
    dispatches = list(
        (
            await db.execute(
                select(evaluation_dispatches).where(
                    evaluation_dispatches.c.run_id == str(scope.run_id)
                )
            )
        )
        .mappings()
        .all()
    )
    slots = project_slots(
        creation["slots"],
        generations,
        dispatches,
        semantics,
        progress.get("result_unknown_resolutions", []),
    )
    action = next_action(
        "collecting",
        progress["preflight"]["status"],
        progress["preflight"]["case_id"],
        slots,
        policy,
        unresolved_unknown=progress.get("unresolved_result_unknown_count", 0),
    )
    if action.kind != "await_review":
        raise CheckpointConflict("Review requires complete frozen candidate evidence")
    closures = [
        t for t in progress["transitions"] if t["cause_code"] == "candidate_evidence_complete"
    ]
    if len(closures) != 1:
        raise CheckpointConflict("Unique candidate evidence closure required")
    closed_at = datetime.fromisoformat(closures[0]["at"])
    semantic_by_id = {r["execution_id"]: r for r in semantics}
    candidates = []
    for row in generations:
        stored = row["candidate_json"]
        if stored is None:
            continue
        generated = decode_completion(row)
        accepted = semantic_by_id[stored["accepted_semantic_execution_id"]]
        candidates.append(
            ReviewCandidate(
                stored["id"],
                generated.finished_at,
                generated.normalized_output,
                decode_semantic_completion(accepted),
                accepted["result_json"]["evaluator_version"],
                tuple(AssertionReceipt(**a) for a in stored["assertions"]),
                tuple(AssertionReceipt(**a) for a in stored["semantic_assertions"]),
            )
        )
    result = add_human_reviews(
        progress["status"],
        closed_at,
        tuple(candidates),
        decode_reviews(progress.get("human_reviews", [])),
        values,
        gate_policy_version=gate.reference.version,
    )
    updated = {
        **progress,
        "human_reviews": [{**asdict(r), "reviewed_at": r.reviewed_at.isoformat()} for r in result],
    }
    state = CheckpointState(scope.run_id, expected_version + 1, None)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(scope.run_id))
        .values(progress_json=updated)
    )
    return state
