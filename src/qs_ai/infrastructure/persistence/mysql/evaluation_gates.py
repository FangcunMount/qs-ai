"""Rebuild all release gates from a scoped, immutable MySQL snapshot."""

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.closure import ClosureTransition, validate_closed_inventory
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence
from qs_ai.domain.evaluation.quality_gates import QualityCandidate, evaluate_quality_gates
from qs_ai.domain.evaluation.review import ReviewCandidate
from qs_ai.infrastructure.persistence.mysql.evaluation_candidates import header
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import (
    decode_completion,
    decode_semantic_completion,
    project_slots,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import decode_resolutions
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import decode_reviews
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_generation_completions,
    evaluation_run_policies,
    evaluation_semantic_completions,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
    load_quality_thresholds,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import load_suite
from qs_ai.infrastructure.qs_server.preflight import run_preflight


async def preview_gates(
    db: AsyncSession, scope: ManagementScope, expected_version: int, at: datetime
) -> GatePreview:
    """Caller authorizes read access; no writes or model calls occur here."""
    if type(expected_version) is not int or expected_version < 1:
        raise ValueError("Explicit Run version required")
    run = await header(db, scope)
    return await evaluate_snapshot(db, scope, dict(run), expected_version, at)


async def evaluate_snapshot(
    db: AsyncSession,
    scope: ManagementScope,
    run: Mapping[str, Any],
    expected_version: int,
    at: datetime,
) -> GatePreview:
    """Use the caller's existing snapshot, or its locked Run and checkpoint."""
    if run["version"] != expected_version:
        raise CheckpointConflict("Gate preview version changed")
    checkpoint = (
        await db.execute(
            select(evaluation_checkpoints.c.checkpoint_json).where(
                evaluation_checkpoints.c.run_id == str(scope.run_id)
            )
        )
    ).scalar_one()
    if checkpoint is not None:
        raise CheckpointConflict("Gate preview requires no active execution")
    creation, progress = json.loads(run["definition_json"]), run["progress_json"]
    if not progress or progress["status"] != "awaiting_review":
        raise CheckpointConflict("Gate preview requires a closed review inventory")
    if (
        type(progress.get("unresolved_result_unknown_count", 0)) is not int
        or progress.get("unresolved_result_unknown_count", 0) != 0
        or progress.get("canceled_at")
        or progress.get("gate_result")
        or progress.get("finalized_at")
    ):
        raise CheckpointConflict("Closed Run progress requires reconciliation")
    if creation["schema_version"] != "qs-ai-evaluation-run-creation/v1" or creation[
        "run_id"
    ] != str(scope.run_id):
        raise ValueError("Frozen Run identity differs from index")
    audit = creation["audit"]
    reason = audit["request_reason"]
    if not reason.strip() or len(reason.encode()) > 1000 or any(c in reason for c in "<>"):
        raise ValueError("Frozen request audit requires a valid reason")
    if (audit["organization_id"], audit["requested_by"]) != (
        scope.organization_id,
        run["requested_by"],
    ):
        raise ValueError("Frozen audit differs from Run scope")
    release = EvidenceReleaseIdentity(
        **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
    )
    policy, gate, suite = load_execution_policy(), load_gate_policy(), load_suite(release.suite)
    release.validate_frozen_policies(
        creation["execution_policy_json"], creation["gate_policy_json"]
    )
    if (
        creation["release_fingerprint"],
        creation["execution_policy_json"],
        creation["gate_policy_json"],
        creation["suite_json"],
    ) != (
        release.fingerprint(),
        policy.definition_json,
        gate.definition_json,
        suite.definition_json,
    ):
        raise ValueError("Frozen release documents differ from registered identity")
    reserved = (
        (
            await db.execute(
                select(evaluation_run_policies).where(
                    evaluation_run_policies.c.run_id == str(scope.run_id)
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if reserved is None or (reserved["fingerprint"], reserved["definition_json"]) != (
        policy.fingerprint,
        policy.definition_json,
    ):
        raise ValueError("Dispatch policy differs from frozen release")
    if creation["slots"] != [
        {"case_id": case, "ordinal": ordinal, "status": "pending"}
        for case, ordinal in suite.slots()
    ]:
        raise ValueError("Frozen slot plan differs from suite")
    if creation["preflight"] != {"case_id": suite.preflight_case_id, "status": "pending"}:
        raise ValueError("Frozen preflight differs from suite")
    raw = progress["preflight"]
    preflight = PreflightEvidence(
        **{
            **raw,
            "evaluated_at": datetime.fromisoformat(raw["evaluated_at"]),
            "assertions": tuple(AssertionReceipt(**a) for a in raw["assertions"]),
        }
    )
    if preflight != run_preflight(release.suite, preflight.evaluated_at):
        raise ValueError("Preflight evidence differs from frozen rejection case")
    generations: list[RowMapping] = []
    semantics: list[RowMapping] = []
    dispatches: list[RowMapping] = []
    for table, records, limit in (
        (evaluation_generation_completions, generations, policy.generation_per_run),
        (evaluation_semantic_completions, semantics, policy.semantic_per_run),
        (evaluation_dispatches, dispatches, policy.generation_per_run + policy.semantic_per_run),
    ):
        records.extend(
            (
                await db.execute(
                    select(table)
                    .where(table.c.run_id == str(scope.run_id))
                    .order_by(table.c.execution_id)
                    .limit(limit + 1)
                )
            ).mappings()
        )
        if len(records) > limit:
            raise ValueError("Persisted execution budget exceeded")
    resolutions = progress.get("result_unknown_resolutions", [])
    slots = project_slots(creation["slots"], generations, dispatches, semantics, resolutions)
    generated = tuple(decode_completion(r) for r in generations)
    judged = tuple(decode_semantic_completion(r) for r in semantics)
    transitions = tuple(
        ClosureTransition(
            t.get("from", ""),
            t["to"],
            t["actor"],
            t["cause_code"],
            datetime.fromisoformat(t["at"]),
            tuple(t.get("evidence_refs", [])),
        )
        for t in progress["transitions"]
    )
    for transition in progress["transitions"]:
        if transition["cause_code"] == "evaluation_started":
            reason = transition.get("reason", "")
            if not reason.strip() or len(reason.encode()) > 1000 or any(c in reason for c in "<>"):
                raise ValueError("Start transition requires an audited reason")
    if creation["transitions"] != progress["transitions"][:1] or creation["status"] != "requested":
        raise ValueError("Initial Run history differs from frozen creation")
    closed = validate_closed_inventory(
        datetime.fromisoformat(audit["created_at"]),
        audit["requested_by"],
        transitions,
        preflight,
        slots,
        generated,
        judged,
        decode_resolutions(resolutions),
        policy,
    )
    for row in dispatches:
        cp = decode(row["checkpoint_json"])
        if cp is None or not preflight.evaluated_at <= cp.claimed_at <= closed:
            raise ValueError("Dispatch preparation is outside evidence collection")
    by_id = {r["execution_id"]: r for r in semantics}
    candidates = []
    for row in generations:
        stored = row["candidate_json"]
        if stored is None:
            continue
        generation = decode_completion(row)
        accepted = by_id[stored["accepted_semantic_execution_id"]]
        candidate = ReviewCandidate(
            stored["id"],
            generation.finished_at,
            generation.normalized_output,
            decode_semantic_completion(accepted),
            accepted["result_json"]["evaluator_version"],
            tuple(AssertionReceipt(**a) for a in stored["assertions"]),
            tuple(AssertionReceipt(**a) for a in stored["semantic_assertions"]),
        )
        candidates.append(
            QualityCandidate(
                generation.case_id, generation.slot_ordinal, generation.execution_id, candidate
            )
        )
    quality = evaluate_quality_gates(
        tuple(candidates),
        generated,
        judged,
        decode_reviews(progress.get("human_reviews", [])),
        load_quality_thresholds(),
        closed,
        at,
    )
    return GatePreview(str(scope.run_id), run["version"], release.fingerprint(), quality)
