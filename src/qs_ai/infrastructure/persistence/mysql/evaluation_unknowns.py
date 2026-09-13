"""Read uncertain calls in one organization-scoped repeatable-read snapshot."""

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.evaluation.unknowns import UnknownExecutionIndex, UnknownExecutionSummary
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution_evidence import (
    load_resolution_evidence,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints


async def list_unknowns(
    db: AsyncSession, scope: ManagementScope, expected_version: int
) -> UnknownExecutionIndex:
    run = await header(db, scope)
    if run["version"] != expected_version:
        raise CheckpointConflict("Unknown-call view version changed")
    checkpoint = (
        await db.execute(
            select(evaluation_checkpoints.c.checkpoint_json).where(
                evaluation_checkpoints.c.run_id == str(scope.run_id)
            )
        )
    ).scalar_one()
    if checkpoint is not None:
        raise CheckpointConflict("Execution remains active; read after recovery")
    receipt = json.loads(creation_receipt(dict(run)))
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    if not progress:
        raise CheckpointConflict("Legacy progress needs reconciliation")
    evidence = await load_resolution_evidence(db, scope.run_id, creation, progress)
    policy = evidence.policy
    if receipt["release"]["execution_policy"] != {
        "id": policy.policy_id,
        "version": policy.version,
        "fingerprint": policy.fingerprint,
    }:
        raise CheckpointConflict("Execution policy differs from frozen release")
    resolved = {r.execution_id for r in evidence.resolutions}
    unresolved = {u.execution_id for u in evidence.unknowns} - resolved
    status = progress["status"]
    if unresolved and status not in ("blocked", "canceled"):
        raise CheckpointConflict("Unresolved calls require blocked or canceled Run")
    can_resolve = status == "blocked" and bool(unresolved)
    dispatches = {r["execution_id"]: r for r in evidence.dispatches}
    summaries = []
    for row in evidence.generations + evidence.semantics:
        if row["execution_id"] not in unresolved:
            continue
        dispatch = dispatches[row["execution_id"]]
        kind = dispatch["kind"]
        stage = [d for d in evidence.dispatches if d["kind"] == kind]
        target = [
            d
            for d in stage
            if (d["case_id"], d["slot_ordinal"], d["candidate_id"])
            == (dispatch["case_id"], dispatch["slot_ordinal"], dispatch["candidate_id"])
        ]
        value = row["evidence_json"]
        failure = value["failure"]
        stage_name = "generation_execution" if kind == "generation" else "semantic_evaluation"
        summaries.append(
            UnknownExecutionSummary(
                execution_id=row["execution_id"],
                invocation_id=row["invocation_id"],
                kind=kind,
                case_id=dispatch["case_id"],
                slot_ordinal=dispatch["slot_ordinal"],
                candidate_id=dispatch["candidate_id"] or "",
                execution_ordinal=row["execution_ordinal"],
                started_at=value["started_at"],
                finished_at=value["finished_at"],
                provider_call_count=value["provider_call_count"],
                failure_stage=failure["stage"],
                failure_code=failure["code"],
                target_execution_count=len(target),
                target_execution_limit=policy.generation_per_slot
                if kind == "generation"
                else policy.semantic_per_candidate,
                stage_execution_count=len(stage),
                stage_execution_limit=policy.generation_per_run
                if kind == "generation"
                else policy.semantic_per_run,
                replacement_allowed=can_resolve
                and policy.within_budget(stage_name, len(target), len(stage)),
            )
        )
    summaries.sort(
        key=lambda item: (
            item.kind,
            item.case_id,
            item.slot_ordinal,
            item.execution_ordinal,
            item.execution_id,
        )
    )
    return UnknownExecutionIndex(
        str(scope.run_id),
        run["version"],
        receipt["release_fingerprint"],
        status,
        len(unresolved),
        can_resolve,
        tuple(summaries),
    )
