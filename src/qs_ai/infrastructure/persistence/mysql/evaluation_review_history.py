"""Rebuild every archived final gate before trusting a subsequent review round."""

import json
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime

from qs_ai.application.evaluation.finalization import final_record, final_transition
from qs_ai.application.evaluation.gates import GatePreview
from qs_ai.domain.evaluation.quality_gates import QualityGateResult
from qs_ai.domain.evaluation.reopening import (
    ReviewReopening,
    reopen_review,
    validate_reopening_history,
)
from qs_ai.domain.evaluation.review import CandidateHumanReview, ReviewCandidate
from qs_ai.infrastructure.persistence.mysql.evaluation_review_codec import decode_reviews


def canonical(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def has_review_rounds(progress: dict) -> bool:
    # A missing or emptied archive must not bypass validation of its transitions.
    return "review_reopenings" in progress or any(
        transition.get("cause_code") == "semantic_review_reopened"
        for transition in progress.get("transitions", [])
    )


def opening_transition(entry: dict) -> dict:
    return {
        "from": "rejected",
        "to": "awaiting_review",
        "cause_code": "semantic_review_reopened",
        "actor": entry["actor"],
        "reason": entry["reason"],
        "at": entry["reopened_at"],
    }


def opening_record(plan: ReviewReopening, final: dict, transition_count: int) -> dict:
    return {
        "source_version": final["version"],
        "version": final["version"] + 1,
        "transition_count": transition_count,
        "previous_finalization": final,
        "previous_reviews": [
            {**asdict(r), "reviewed_at": r.reviewed_at.isoformat()} for r in plan.previous_reviews
        ],
        "candidate_ids": list(plan.candidate_ids),
        "actor": plan.actor,
        "reason": plan.reason,
        "reopened_at": plan.reopened_at.isoformat(),
    }


def validate_rounds(
    run_id: str,
    version: int,
    fingerprint: str,
    progress: dict,
    closure_count: int,
    candidates: tuple[ReviewCandidate, ...],
    closed_at: datetime,
    at: datetime,
    calculate: Callable[[tuple[CandidateHumanReview, ...], datetime], QualityGateResult],
) -> None:
    raw_history = progress.get("review_reopenings", [])
    if not isinstance(raw_history, list) or len(raw_history) > 3:
        raise ValueError("Bounded review history required")
    if len(canonical(raw_history).encode()) > 2 * 1024 * 1024:
        raise ValueError("Review history exceeds response bound")
    transitions = progress["transitions"]
    if len(transitions) != closure_count + 2 * len(raw_history):
        raise ValueError("Review transitions differ from archived rounds")
    history = []
    previous_version = 1
    for index, entry in enumerate(raw_history):
        final = entry["previous_finalization"]
        source_version = final["source_version"]
        if type(source_version) is not int or source_version < previous_version:
            raise ValueError("Historical finalization version is invalid")
        reviews = decode_reviews(entry["previous_reviews"])
        quality = calculate(reviews, datetime.fromisoformat(final["finalized_at"]))
        expected_final = final_record(
            GatePreview(run_id, source_version, fingerprint, quality),
            final["actor"],
            final["reason"],
        )
        if canonical(final) != canonical(expected_final):
            raise ValueError("Historical final gate differs from original evidence")
        plan = reopen_review(
            candidates,
            reviews,
            quality,
            closed_at,
            entry["actor"],
            entry["reason"],
            datetime.fromisoformat(entry["reopened_at"]),
            status=final["status"],
            gate_policy_version="v2",
            reopening_count=index,
        )
        boundary = closure_count + index * 2 + 1
        expected = opening_record(plan, expected_final, boundary)
        if (
            canonical(entry) != canonical(expected)
            or transitions[boundary - 1] != final_transition(final)
            or transitions[boundary] != opening_transition(expected)
            or expected["version"] > version
            or plan.reopened_at > at
        ):
            raise ValueError("Historical reopening audit differs from its Run boundary")
        previous_version = expected["version"]
        history.append(plan)
    validate_reopening_history(
        tuple(history),
        decode_reviews(progress.get("human_reviews", [])),
        candidates,
        closed_at,
        gate_policy_version="v2",
    )
