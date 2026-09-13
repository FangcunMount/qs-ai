"""General cancellation competes with dispatch under the same Run/checkpoint CAS."""

import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.actions import next_action
from qs_ai.domain.evaluation.cancellation import CancellationDecision
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import (
    read_finalization,
    save_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import project_slots
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution_evidence import (
    load_resolution_evidence,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import canonical
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


def transition(record: dict) -> dict:
    return {
        "from": record["source_status"],
        "to": "canceled",
        "cause_code": "operator_discarded" if record["discard"] else "operator_canceled",
        "actor": record["actor"],
        "reason": record["reason"],
        "at": record["canceled_at"],
        "evidence_refs": [record["execution_id"]] if record["execution_id"] else [],
    }


async def record_for(
    db: AsyncSession,
    scope: ManagementScope,
    run: dict,
    checkpoint: dict | None,
    decision: CancellationDecision,
) -> dict:
    creation = json.loads(run["definition_json"])
    receipt = json.loads(creation_receipt(run))
    progress = run["progress_json"]
    if not progress or progress.get("cancellation") or progress.get("canceled_at"):
        raise CheckpointConflict("Cancellation requires current uncanceled progress")
    cp = decode(checkpoint)
    try:
        decision.cause(progress["status"], progress.get("unresolved_result_unknown_count", 0), cp)
    except ValueError as error:
        raise CheckpointConflict(str(error)) from None
    evidence = await load_resolution_evidence(db, scope.run_id, creation, progress)
    # Counts are reconstructed from original completion/dispatch/resolution evidence above.
    times = [receipt["created_at"], *(t["at"] for t in progress["transitions"])]
    times += [r["evidence_json"]["finished_at"] for r in evidence.generations + evidence.semantics]
    times += [r["reviewed_at"] for r in progress.get("human_reviews", [])]
    if any(decision.canceled_at < datetime.fromisoformat(value) for value in times):
        raise ValueError("Cancellation cannot precede existing evidence")
    if progress["status"] == "requested" and evidence.dispatches:
        raise CheckpointConflict("Requested Run cannot own dispatched work")
    if cp is not None:
        slots = project_slots(
            creation["slots"],
            evidence.generations,
            evidence.dispatches,
            evidence.semantics,
            progress.get("result_unknown_resolutions", []),
        )
        preflight = progress.get("preflight", creation["preflight"])
        planned = next_action(
            progress["status"], preflight["status"], preflight["case_id"], slots, evidence.policy
        )
        if any(
            getattr(cp, key) != getattr(planned, key)
            for key in ("kind", "case_id", "slot_ordinal", "candidate_id", "execution_ordinal")
        ):
            raise CheckpointConflict("Prepared checkpoint differs from frozen execution plan")
    await read_finalization(db, scope, run)
    return {
        "schema_version": "qs-ai-evaluation-cancellation/v1",
        "run_id": str(scope.run_id),
        "source_version": run["version"],
        "version": run["version"] + 1,
        "source_status": progress["status"],
        "status": "canceled",
        "release_fingerprint": receipt["release_fingerprint"],
        "actor": decision.actor,
        "reason": decision.reason,
        "discard": decision.discard,
        "canceled_at": decision.canceled_at.isoformat(),
        "execution_id": cp.execution_id if cp else "",
        "invocation_id": cp.invocation_id if cp else "",
    }


async def cancel(
    db: AsyncSession,
    scope: ManagementScope,
    expected_version: int,
    reason: str,
    at: datetime,
    *,
    discard: bool,
    confirm: bool,
) -> None:
    if (
        confirm is not True
        or type(expected_version) is not int
        or not 0 < expected_version < 2**63 - 1
    ):
        raise ValueError("Explicit version and confirmation required")
    decision = CancellationDecision(scope.actor, reason.strip(), discard, at)
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
    if run is None:
        raise NotFound("Evaluation unavailable in organization")
    if checkpoint is None or checkpoint["version"] != expected_version:
        raise CheckpointConflict("Cancellation version changed")
    original = {**run, "version": expected_version}
    record = await record_for(db, scope, original, checkpoint["checkpoint_json"], decision)
    progress = run["progress_json"]
    await save_progress(
        db,
        scope,
        expected_version,
        {
            **progress,
            "status": "canceled",
            "canceled_at": record["canceled_at"],
            "cancellation": record,
            # Preserve the exact prepared checkpoint as audit evidence, never as resumable work.
            "canceled_checkpoint": checkpoint["checkpoint_json"],
            "transitions": [*progress["transitions"], transition(record)],
        },
    )


async def read_cancellation(
    db: AsyncSession, scope: ManagementScope, run: dict
) -> tuple[str, dict]:
    """Return the audited receipt plus original source state for historical-review validation."""
    progress = run["progress_json"]
    record = progress.get("cancellation")
    if record is None:
        if progress.get("canceled_checkpoint") is not None:
            raise ValueError("Canceled preparation requires its original audit")
        return "", run
    if (
        not isinstance(record, dict)
        or progress["status"] != "canceled"
        or run.get("checkpoint_json") is not None
        or record.get("source_version") != run["version"] - 1
        or type(record.get("source_version")) is not int
        or record["source_version"] < 1
        or record.get("version") != run["version"]
        or progress.get("canceled_at") != record.get("canceled_at")
        or not progress["transitions"]
        or progress["transitions"][-1] != transition(record)
    ):
        raise ValueError("Cancellation audit requires reconciliation")
    original_progress = {
        **progress,
        "status": record["source_status"],
        "transitions": progress["transitions"][:-1],
    }
    for key in ("cancellation", "canceled_checkpoint", "canceled_at"):
        original_progress.pop(key, None)
    original = {**run, "version": record["source_version"], "progress_json": original_progress}
    decision = CancellationDecision(
        record["actor"],
        record["reason"],
        record["discard"],
        datetime.fromisoformat(record["canceled_at"]),
    )
    expected = await record_for(db, scope, original, progress.get("canceled_checkpoint"), decision)
    if canonical(record) != canonical(expected):
        raise ValueError("Cancellation differs from its source evidence")
    return canonical(record), original
