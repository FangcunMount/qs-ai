"""Organization-scoped candidate reads from one repeatable-read snapshot."""

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.candidates import (
    CandidateEvidence,
    CandidateIndex,
    CandidateSummary,
)
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.adjudication import effective_candidate_assertions
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.review import ReviewCandidate
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import prepare_run_case
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import read_cancellation
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import read_finalization
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import (
    decode_completion,
    decode_semantic_completion,
    project_slots,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_review_codec import decode_reviews
from qs_ai.infrastructure.persistence.mysql.evaluation_review_history import has_review_rounds
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions as generations,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_semantic_completions as semantics,
)


async def list_candidates(db: AsyncSession, scope: ManagementScope) -> CandidateIndex:
    run = await header(db, scope)
    rows = (
        (
            await db.execute(
                select(
                    generations.c.candidate_id, generations.c.case_id, generations.c.slot_ordinal
                )
                .where(
                    generations.c.run_id == str(scope.run_id),
                    generations.c.candidate_id.is_not(None),
                )
                .order_by(generations.c.case_id, generations.c.slot_ordinal)
                .limit(36)
            )
        )
        .mappings()
        .all()
    )
    if len(rows) > 35 or len({r["candidate_id"] for r in rows}) != len(rows):
        raise CheckpointConflict("Candidate index requires reconciliation")
    return CandidateIndex(
        str(scope.run_id), run["version"], tuple(CandidateSummary(**r) for r in rows)
    )


async def get_candidate(
    db: AsyncSession, scope: ManagementScope, candidate_id: str, expected_version: int
) -> CandidateEvidence:
    run = await header(db, scope)
    if run["version"] != expected_version:
        raise CheckpointConflict("Candidate view version changed")
    source = dict(run)
    if run["progress_json"] is not None:
        _, source = await read_cancellation(db, scope, source)
    if has_review_rounds(source["progress_json"] or {}):
        await read_finalization(db, scope, source)
    selected = (
        (
            await db.execute(
                select(generations).where(
                    generations.c.run_id == str(scope.run_id),
                    generations.c.candidate_id == candidate_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if selected is None:
        raise NotFound("Candidate unavailable in evaluation")
    creation = json.loads(run["definition_json"])
    release = EvidenceReleaseIdentity(
        **{key: FrozenContractRef(**ref) for key, ref in creation["release"].items()}
    )
    if release.fingerprint() != creation["release_fingerprint"]:
        raise CheckpointConflict("Frozen release identity differs from fingerprint")
    slot = {"case_id": selected["case_id"], "ordinal": selected["slot_ordinal"]}
    if sum(all(s[k] == v for k, v in slot.items()) for s in creation["slots"]) != 1:
        raise CheckpointConflict("Candidate outside frozen slots")
    records = list(
        (
            await db.execute(
                select(generations).where(
                    generations.c.run_id == str(scope.run_id),
                    generations.c.case_id == slot["case_id"],
                    generations.c.slot_ordinal == slot["ordinal"],
                )
            )
        ).mappings()
    )
    semantic_records = list(
        (
            await db.execute(
                select(semantics).where(
                    semantics.c.run_id == str(scope.run_id),
                    semantics.c.candidate_id == candidate_id,
                )
            )
        ).mappings()
    )
    dispatches = list(
        (
            await db.execute(
                select(evaluation_dispatches).where(
                    evaluation_dispatches.c.run_id == str(scope.run_id),
                    evaluation_dispatches.c.case_id == slot["case_id"],
                    evaluation_dispatches.c.slot_ordinal == slot["ordinal"],
                )
            )
        ).mappings()
    )
    execution_ids = {r["execution_id"] for r in records + semantic_records}
    progress = run["progress_json"] or {}
    resolutions = [
        r
        for r in progress.get("result_unknown_resolutions", [])
        if r["execution_id"] in execution_ids
    ]
    recoveries = [
        r
        for r in progress.get("semantic_contract_recoveries", [])
        if r["execution_id"] in execution_ids
    ]
    projected = project_slots(
        [slot], records, dispatches, semantic_records, resolutions, recoveries
    )[0]
    if projected.candidate is None or not projected.candidate.review_ready:
        raise CheckpointConflict("Candidate semantic evidence is not complete")
    generated = decode_completion(selected)
    candidate = selected["candidate_json"]
    accepted = next(
        r
        for r in semantic_records
        if r["execution_id"] == candidate["accepted_semantic_execution_id"]
    )
    judged = decode_semantic_completion(accepted)
    reviews = [r for r in progress.get("human_reviews", []) if r["candidate_id"] == candidate_id]
    closures = [
        t
        for t in progress.get("transitions", [])
        if t["cause_code"] == "candidate_evidence_complete"
    ]
    if reviews and len(closures) != 1:
        raise CheckpointConflict("Review closure requires reconciliation")
    closed_at = datetime.fromisoformat(closures[0]["at"]) if closures else judged.finished_at
    effective = effective_candidate_assertions(
        ReviewCandidate(
            candidate_id,
            generated.finished_at,
            generated.normalized_output,
            judged,
            accepted["result_json"]["evaluator_version"],
            tuple(AssertionReceipt(**a) for a in candidate["assertions"]),
            tuple(AssertionReceipt(**a) for a in candidate["semantic_assertions"]),
        ),
        decode_reviews(reviews),
        closed_at,
        gate_policy_version=release.gate_policy.version,
    )

    # Only expose accepted receipt fields, excluding raw provider responses and prompts.
    def receipt(value: GenerationCompletion | SemanticCompletion) -> dict[str, Any]:
        return {
            "execution_id": value.execution_id,
            "invocation_id": value.invocation_id,
            "started_at": value.started_at.isoformat(),
            "finished_at": value.finished_at.isoformat(),
            "receipt": asdict(value.receipt) if value.receipt is not None else None,
        }

    frozen_input: dict[str, Any] = {"available": False, "reason": "frozen_input_unavailable"}
    try:
        prepared = await prepare_run_case(db, creation, selected["case_id"])
        frozen_input = {
            "available": True,
            "case_id": selected["case_id"],
            "fingerprint": prepared.assembled_input.fingerprint,
            "content": json.loads(prepared.assembled_input.canonical_json),
        }
    except (ValueError, KeyError, NotFound):
        # A historical candidate can remain inspectable while its input is unavailable.
        # No missing evidence is interpreted as a passed rule or replaced with new facts.
        pass
    evidence = json.dumps(
        {
            "frozen_input": frozen_input,
            "release": asdict(release),
            "release_fingerprint": release.fingerprint(),
            "candidate": candidate,
            "generation": receipt(generated),
            "semantic": {
                **receipt(judged),
                "output_fingerprint": judged.output_fingerprint,
                "result": accepted["result_json"],
            },
            "reviews": reviews,
            "effective_assertions": [asdict(a) for a in effective.assertions],
            "semantic_adjudication": asdict(effective.adjudication)
            if effective.adjudication
            else None,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    if (
        len(evidence.encode()) + len(generated.normalized_output) + len(judged.normalized_output)
        > 2 * 1024 * 1024
    ):
        raise CheckpointConflict("Candidate evidence exceeds read bound")
    return CandidateEvidence(
        str(scope.run_id),
        run["version"],
        candidate_id,
        generated.normalized_output,
        judged.normalized_output,
        evidence,
    )
