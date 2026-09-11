"""Accept judge completion and candidate decisions in one caller-owned transaction."""

import json
from dataclasses import asdict
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.release import resolve_semantic_route
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.domain.evaluation.actions import next_action
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import (
    decode_completion,
    project_slots,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_generation_completions,
    evaluation_runs,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_semantic_completions as table,
)
from qs_ai.infrastructure.qs_server.evaluation_assertions import (
    assertion_inventory,
    semantic_obligations,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from qs_ai.infrastructure.qs_server.semantic_output import parse_semantic_output


async def complete_semantic(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    completion: SemanticCompletion,
    routes: RouteAssets,
) -> CheckpointState:
    checkpoint = (
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
    if checkpoint is None or checkpoint["version"] != expected_version:
        raise CheckpointConflict("Run version changed")
    cp = decode(checkpoint["checkpoint_json"])
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
    if run is None or not run["progress_json"] or run["progress_json"]["status"] != "collecting":
        raise CheckpointConflict("Collecting Run in organization required")
    row = (
        (
            await db.execute(
                select(evaluation_generation_completions)
                .where(
                    evaluation_generation_completions.c.run_id == str(run_id),
                    evaluation_generation_completions.c.candidate_id == completion.candidate_id,
                )
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or not row["candidate_json"]:
        raise CheckpointConflict("Accepted candidate required")
    generated = decode_completion(row)
    candidate = dict(row["candidate_json"])
    if (
        generated.status != "succeeded"
        or candidate["review_ready"]
        or cp is None
        or not completion.matches_checkpoint(cp, owner, generated.normalized_fingerprint)
        or (cp.case_id, cp.slot_ordinal) != (generated.case_id, generated.slot_ordinal)
    ):
        raise CheckpointConflict("Semantic completion does not match current candidate")
    if (
        candidate["id"],
        candidate["generation_execution_id"],
        candidate["normalized_output_fingerprint"],
    ) != (completion.candidate_id, generated.execution_id, generated.normalized_fingerprint):
        raise CheckpointConflict("Candidate identity or output evidence mismatch")
    ledger = (
        await db.execute(
            select(evaluation_dispatches.c.checkpoint_json)
            .where(
                evaluation_dispatches.c.run_id == str(run_id),
                evaluation_dispatches.c.invocation_id == completion.invocation_id,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    dispatched = decode(ledger)
    if (
        dispatched is None
        or not completion.matches_checkpoint(dispatched, owner, generated.normalized_fingerprint)
        or (dispatched.case_id, dispatched.slot_ordinal) != (cp.case_id, cp.slot_ordinal)
    ):
        raise CheckpointConflict("Matching semantic dispatch reservation required")
    previous = (
        (
            await db.execute(
                select(table)
                .where(
                    table.c.run_id == str(run_id), table.c.candidate_id == completion.candidate_id
                )
                .order_by(table.c.execution_ordinal)
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    if [r["execution_ordinal"] for r in previous] != list(
        range(1, completion.execution_ordinal)
    ) or any(r["result_json"] is not None for r in previous):
        raise CheckpointConflict("Semantic sequence already accepted or incomplete")
    creation = json.loads(run["definition_json"])
    policy = load_execution_policy()
    if creation["execution_policy_json"] != policy.definition_json:
        raise CheckpointConflict("Unsupported frozen execution policy")
    release = EvidenceReleaseIdentity(
        **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
    )
    route = await resolve_semantic_route(release, routes)
    definition = json.loads(route.definition_json)
    if completion.receipt is not None and (
        completion.receipt.provider,
        completion.receipt.model,
    ) != (definition["provider"], definition["model"]):
        raise ValueError("Semantic receipt does not match frozen route")
    result = None
    if completion.status == "succeeded":
        assert completion.receipt is not None
        obligations = semantic_obligations(
            assertion_inventory(release.suite, generated.case_id),
            tuple(AssertionReceipt(**a) for a in candidate["assertions"]),
        )
        result = await parse_semantic_output(
            completion.normalized_output,
            release,
            routes,
            completion.receipt,
            completion.invocation_id,
            obligations,
        )
        resolved = {(a.type, a.scope, a.ordinal): asdict(a) for a in result.decisions}
        candidate["assertions"] = [
            resolved[(a["type"], a["scope"], a["ordinal"])]
            if a["status"] == "pending_semantic"
            else a
            for a in candidate["assertions"]
        ]
        candidate["semantic_assertions"] = [asdict(a) for a in result.decisions]
        candidate["review_ready"] = True
        candidate["accepted_semantic_execution_id"] = completion.execution_id
    evidence = asdict(completion)
    for key in ("raw_output", "normalized_output"):
        del evidence[key]
    for key in ("started_at", "finished_at"):
        evidence[key] = evidence[key].isoformat()
    evidence["output_fingerprint"] = completion.output_fingerprint
    await db.execute(
        insert(table).values(
            run_id=str(run_id),
            execution_id=completion.execution_id,
            invocation_id=completion.invocation_id,
            candidate_id=completion.candidate_id,
            execution_ordinal=completion.execution_ordinal,
            evidence_json=evidence,
            result_json=asdict(result) if result is not None else None,
            raw_output=completion.raw_output,
            normalized_output=completion.normalized_output,
        )
    )
    if result is not None:
        await db.execute(
            update(evaluation_generation_completions)
            .where(
                evaluation_generation_completions.c.run_id == str(run_id),
                evaluation_generation_completions.c.execution_id == generated.execution_id,
            )
            .values(candidate_json=candidate)
        )
    progress = dict(run["progress_json"])
    cause = ""
    if completion.status == "result_unknown":
        progress["unresolved_result_unknown_count"] = (
            progress.get("unresolved_result_unknown_count", 0) + 1
        )
        cause = "result_unknown_requires_review"
    elif completion.status == "failed":
        if completion.execution_ordinal >= policy.semantic_per_candidate:
            cause = "semantic_budget_exhausted"
        elif completion.failure is None or not policy.allows_automatic_semantic_recovery(
            completion.failure
        ):
            cause = "semantic_recovery_not_allowed"
    if cause:
        progress["status"] = "blocked"
        progress["transitions"] = [
            *progress["transitions"],
            {
                "from": "collecting",
                "to": "blocked",
                "actor": owner.strip(),
                "cause_code": cause,
                "at": completion.finished_at.isoformat(),
                "evidence_refs": [completion.execution_id],
            },
        ]
    if result is not None:
        generations = (
            (
                await db.execute(
                    select(evaluation_generation_completions)
                    .where(evaluation_generation_completions.c.run_id == str(run_id))
                    .with_for_update()
                )
            )
            .mappings()
            .all()
        )
        semantics = (
            (await db.execute(select(table).where(table.c.run_id == str(run_id)).with_for_update()))
            .mappings()
            .all()
        )
        dispatches = (
            (
                await db.execute(
                    select(evaluation_dispatches)
                    .where(evaluation_dispatches.c.run_id == str(run_id))
                    .with_for_update()
                )
            )
            .mappings()
            .all()
        )
        slots = project_slots(
            creation["slots"], list(generations), list(dispatches), list(semantics)
        )
        preflight = progress["preflight"]
        action = next_action(
            progress["status"],
            preflight["status"],
            preflight["case_id"],
            slots,
            policy,
            unresolved_unknown=progress.get("unresolved_result_unknown_count", 0),
        )
        if action.kind == "await_review":
            progress["status"] = "awaiting_review"
            progress["transitions"] = [
                *progress["transitions"],
                {
                    "from": "collecting",
                    "to": "awaiting_review",
                    "actor": owner.strip(),
                    "cause_code": "candidate_evidence_complete",
                    "at": completion.finished_at.isoformat(),
                    "evidence_refs": [completion.execution_id],
                },
            ]
    state = CheckpointState(run_id, expected_version + 1, None)
    await save_checkpoint(db, state, expected_version)
    await db.execute(
        update(evaluation_runs)
        .where(evaluation_runs.c.run_id == str(run_id))
        .values(progress_json=progress)
    )
    return state
