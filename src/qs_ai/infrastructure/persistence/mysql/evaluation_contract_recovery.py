"""A narrow operator-authorized recovery transaction, not a status-editing shortcut."""

import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.actions import next_action
from qs_ai.domain.evaluation.contract_recovery import (
    ContractRecovery,
    decode_recoveries,
    validate_recoveries,
)
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import (
    decode_semantic_completion,
    project_slots,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution_evidence import (
    load_resolution_evidence,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def authorize_contract_recovery(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    expected_release_fingerprint: str,
    value: ContractRecovery,
    *,
    confirm: bool,
) -> CheckpointState:
    """Authorized operator supplies audit; caller commits. All original evidence is immutable."""
    if confirm is not True:
        raise ValueError("Explicit policy exception confirmation required")
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
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    release = EvidenceReleaseIdentity(
        **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
    )
    if (
        creation["release_fingerprint"] != expected_release_fingerprint
        or release.fingerprint() != expected_release_fingerprint
    ):
        raise CheckpointConflict("Frozen release differs from operator plan")
    transitions = progress.get("transitions", [])
    if (
        progress["status"] != "blocked"
        or not transitions
        or transitions[-1]["cause_code"] != "semantic_recovery_not_allowed"
        or transitions[-1].get("evidence_refs") != [value.execution_id]
        or value.resolved_at < datetime.fromisoformat(transitions[-1]["at"])
    ):
        raise ValueError("Only the current semantic contract block can be recovered")
    evidence = await load_resolution_evidence(db, run_id, creation, progress)
    if evidence.unknowns and len(evidence.unknowns) != len(evidence.resolutions):
        raise ValueError("Unresolved calls cannot be retried as contract failures")
    prior = decode_recoveries(progress.get("semantic_contract_recoveries", []))
    values = (*prior, value)
    semantics = tuple(decode_semantic_completion(r) for r in evidence.semantics)
    validate_recoveries(values, semantics, evidence.policy)
    target = next(s for s in semantics if s.execution_id == value.execution_id)
    if target.finished_at != datetime.fromisoformat(transitions[-1]["at"]):
        raise ValueError("Block transition differs from failed execution")
    if (
        any(
            s.candidate_id == target.candidate_id and s.execution_ordinal > target.execution_ordinal
            for s in semantics
        )
        or len(semantics) >= evidence.policy.semantic_per_run
    ):
        raise ValueError("Already retried execution or exhausted Run budget")
    serialized = [{**asdict(r), "resolved_at": r.resolved_at.isoformat()} for r in values]
    slots = project_slots(
        creation["slots"],
        evidence.generations,
        evidence.dispatches,
        evidence.semantics,
        progress.get("result_unknown_resolutions", []),
        serialized,
    )
    preflight = progress["preflight"]
    action = next_action(
        "collecting", preflight["status"], preflight["case_id"], slots, evidence.policy
    )
    if (action.kind, action.candidate_id, action.execution_ordinal, action.cause) != (
        "semantic",
        value.candidate_id,
        target.execution_ordinal + 1,
        "semantic_contract_recovery_approved",
    ):
        raise ValueError("Recovery would not resume the exact failed semantic execution")
    updated = {
        **progress,
        "status": "collecting",
        "semantic_contract_recoveries": serialized,
        "transitions": [
            *transitions,
            {
                "from": "blocked",
                "to": "collecting",
                "cause_code": "semantic_contract_recovery_approved",
                "actor": value.actor,
                "at": value.resolved_at.isoformat(),
                "evidence_refs": [value.execution_id],
            },
        ],
    }
    state = CheckpointState(run_id, expected_version + 1, None)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(progress_json=updated)
    )
    return state
