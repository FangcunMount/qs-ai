"""Accept terminal generation evidence under the shared Run lock and version."""

import asyncio
import json
import re
from dataclasses import asdict
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.completion import validate_generation_completion
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
    complete_claim,
    completion_owner,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, save_checkpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_frozen_policies import frozen_policies
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_runs,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions as table,
)


async def complete_generation(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    completion: GenerationCompletion,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    candidate_id: str = "",
    assertions: tuple[AssertionReceipt, ...] = (),
    claim: SlotClaim | None = None,
) -> CheckpointState:
    """Caller owns commit/rollback and trusted assertion computation; no model call here."""
    candidate = None
    if completion.status == "succeeded":
        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", candidate_id) is None
            or not isinstance(assertions, tuple)
            or not assertions
            or not all(isinstance(a, AssertionReceipt) for a in assertions)
        ):
            raise ValueError("Successful generation requires candidate assertions")
        candidate = {
            "id": candidate_id,
            "generation_execution_id": completion.execution_id,
            "normalized_output_fingerprint": completion.normalized_fingerprint,
            "accepted_at": completion.finished_at.isoformat(),
            "assertions": [asdict(a) for a in assertions],
            "review_ready": False,
        }
    elif candidate_id or assertions:
        raise ValueError("Failed generation cannot create candidate evidence")
    run: dict | RowMapping | None
    cp: ExecutionCheckpoint | None
    if claim is not None:
        run, expected_version, claim = await completion_owner(db, run_id, organization_id, claim)
        cp = claim.checkpoint
        if not completion.matches_checkpoint(cp, owner):
            raise CheckpointConflict("Completion differs from candidate ownership")
    else:
        row = (
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
        cp = decode(row["checkpoint_json"]) if row is not None else None
        if row is None or cp is None or not completion.matches_checkpoint(cp, owner):
            raise CheckpointConflict("Terminal evidence does not match active Run checkpoint")
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
        if (
            run is None
            or run["progress_json"] is None
            or run["progress_json"]["status"] != "collecting"
        ):
            raise CheckpointConflict("Collecting Run in organization required")
    if claim is None:
        assert row is not None
    if claim is None and row is not None and row["version"] != expected_version:
        intent = run["progress_json"].get("cancel_requested", {})
        if (
            intent.get("source_version") != expected_version
            or intent.get("version") != row["version"]
        ):
            raise CheckpointConflict("Run version changed outside cancellation")
        expected_version = row["version"]
    creation = json.loads(run["definition_json"])
    policy, _ = await asyncio.to_thread(frozen_policies, creation)
    if creation["execution_policy_json"] != policy.definition_json:
        raise CheckpointConflict("Unsupported frozen execution policy")
    if not any(
        s["case_id"] == completion.case_id and s["ordinal"] == completion.slot_ordinal
        for s in creation["slots"]
    ):
        raise CheckpointConflict("Completion target is not a frozen slot")
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
    if dispatched is None or not completion.matches_checkpoint(dispatched, owner):
        raise CheckpointConflict("Matching dispatch reservation required")
    previous = (
        (
            await db.execute(
                select(table)
                .where(
                    table.c.run_id == str(run_id),
                    table.c.case_id == completion.case_id,
                    table.c.slot_ordinal == completion.slot_ordinal,
                )
                .order_by(table.c.execution_ordinal)
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    if any(r["candidate_id"] is not None for r in previous) or [
        r["execution_ordinal"] for r in previous
    ] != list(range(1, completion.execution_ordinal)):
        raise CheckpointConflict("Slot already accepted or earlier terminal evidence missing")
    release = EvidenceReleaseIdentity(
        **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
    )
    await validate_generation_completion(release, completion, routes, schemas)
    evidence = asdict(completion)
    if completion.receipt is not None:
        evidence["receipt"] = completion.receipt.definition()
    for name in ("raw_output", "normalized_output"):
        del evidence[name]
    for name in ("started_at", "finished_at"):
        evidence[name] = evidence[name].isoformat()
    await db.execute(
        insert(table).values(
            run_id=str(run_id),
            execution_id=completion.execution_id,
            invocation_id=completion.invocation_id,
            case_id=completion.case_id,
            slot_ordinal=completion.slot_ordinal,
            execution_ordinal=completion.execution_ordinal,
            candidate_id=candidate_id or None,
            candidate_json=candidate,
            evidence_json=evidence,
            raw_output=completion.raw_output,
            normalized_output=completion.normalized_output,
        )
    )
    progress = dict(run["progress_json"])
    cause = ""
    if completion.status == "result_unknown":
        progress["unresolved_result_unknown_count"] = (
            progress.get("unresolved_result_unknown_count", 0) + 1
        )
        cause = "result_unknown_requires_review"
    elif completion.status == "failed":
        if completion.execution_ordinal >= policy.generation_per_slot:
            cause = "generation_budget_exhausted"
        elif completion.failure is None or not policy.allows_automatic_generation_recovery(
            completion.failure
        ):
            cause = "generation_recovery_not_allowed"
    if claim is not None:
        return await complete_claim(
            db, run_id, expected_version, claim, progress, owner, completion.finished_at
        )
    if cause:
        progress["status"] = "blocked"
        progress["transitions"] = [
            *progress["transitions"],
            {
                "from": "collecting",
                "to": "blocked",
                "cause_code": cause,
                "actor": owner.strip(),
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


async def complete_evaluated_generation(
    db: AsyncSession,
    run_id: UUID,
    expected_version: int,
    organization_id: int,
    owner: str,
    completion: GenerationCompletion,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    candidate_id: str = "",
    claim: SlotClaim | None = None,
) -> CheckpointState:
    """Compute original case assertions; callers cannot supply a passing assertion list."""
    from qs_ai.infrastructure.persistence.mysql.evaluation_assets import (
        prepare_run_case,
        stored_run_suite,
    )
    from qs_ai.infrastructure.qs_server.candidate_assertions import evaluate_candidate_assertions

    assertions: tuple[AssertionReceipt, ...] = ()
    if completion.status == "succeeded":
        raw = (
            await db.execute(
                select(evaluation_runs.c.definition_json).where(
                    evaluation_runs.c.run_id == str(run_id),
                    evaluation_runs.c.organization_id == organization_id,
                )
            )
        ).scalar_one_or_none()
        if raw is None:
            raise CheckpointConflict("Run unavailable in organization")
        creation = json.loads(raw)
        release = EvidenceReleaseIdentity(
            **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
        )
        prepared = await prepare_run_case(db, creation, completion.case_id)
        assertions = await asyncio.to_thread(
            evaluate_candidate_assertions,
            completion.normalized_output,
            prepared,
            release.suite,
            completion.case_id,
            frozen_suite=await stored_run_suite(db, creation),
        )
    return await complete_generation(
        db,
        run_id,
        expected_version,
        organization_id,
        owner,
        completion,
        routes,
        schemas,
        candidate_id=candidate_id,
        assertions=assertions,
        claim=claim,
    )
