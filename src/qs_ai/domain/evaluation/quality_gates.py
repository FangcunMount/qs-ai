"""Frozen v2 G3-G5 calculation; release identity/completeness must be checked separately."""

import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.adjudication import (
    SemanticAdjudication,
    effective_candidate_assertions,
)
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.review import CandidateHumanReview, ReviewCandidate, add_human_reviews
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion

SCORE_NAMES = (
    "faithfulness",
    "cross_dimension_quality",
    "suggestion_actionability",
    "audience_clarity",
    "concision",
)


@dataclass(frozen=True)
class QualityThresholds:
    generation_cases: int
    candidates_per_case: int
    review_count: int
    min_infrastructure_rate: float
    min_contract_rate: float
    min_semantic_rate: float
    min_case_passes: int
    min_overall_passes: int
    minimum_scores: tuple[float, ...]
    minimum_averages: tuple[float, ...]

    def __post_init__(self) -> None:
        counts = (
            self.generation_cases,
            self.candidates_per_case,
            self.review_count,
            self.min_case_passes,
            self.min_overall_passes,
        )
        if any(type(n) is not int or n < 1 for n in counts):
            raise ValueError("Invalid frozen candidate and review counts")
        size = self.generation_cases * self.candidates_per_case
        if (
            self.review_count != size * 2
            or self.min_case_passes > self.candidates_per_case
            or self.min_overall_passes > size
        ):
            raise ValueError("Invalid frozen candidate and review counts")
        rates = (self.min_infrastructure_rate, self.min_contract_rate, self.min_semantic_rate)
        if any(
            type(v) not in (int, float) or not math.isfinite(v) or not 0 <= v <= 1 for v in rates
        ):
            raise ValueError("Invalid frozen reliability rates")
        for values in (self.minimum_scores, self.minimum_averages):
            if (
                not isinstance(values, tuple)
                or len(values) != len(SCORE_NAMES)
                or any(
                    type(v) not in (int, float) or not math.isfinite(v) or not 1 <= v <= 5
                    for v in values
                )
            ):
                raise ValueError("Invalid frozen semantic thresholds")


@dataclass(frozen=True)
class QualityCandidate:
    case_id: str
    slot_ordinal: int
    generation_execution_id: str
    evidence: ReviewCandidate


@dataclass(frozen=True)
class GateReason:
    gate: str
    code: str
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class GateMetric:
    name: str
    numerator: int
    denominator: int
    value: float
    threshold: float


@dataclass(frozen=True)
class QualityGateResult:
    """Only G3-G5. This result alone never authorizes approval or publication."""

    evaluated_at: datetime
    gate_passes: tuple[tuple[str, bool], ...]
    metrics: tuple[GateMetric, ...]
    reasons: tuple[GateReason, ...]
    semantic_adjudications: tuple[SemanticAdjudication, ...]


def assertion_outcome(assertions: tuple[AssertionReceipt, ...]) -> tuple[bool, bool, bool]:
    groups: dict[tuple[str, str, int], list[bool]] = {}
    for a in assertions:
        value = groups.setdefault((a.scope, a.type, a.ordinal), [False, False, False])
        value[0] |= a.hard or a.scope == "default"
        value[1] |= a.status == "passed"
        value[2] |= a.status in ("failed", "blocked")
    case_present, case_passed, hard_passed = False, True, True
    for (scope, _, _), (hard, passed, failed) in groups.items():
        effective = passed and not failed
        if scope == "case":
            case_present = True
            case_passed &= effective
        if hard:
            hard_passed &= effective
    return case_present, case_passed, hard_passed


def infrastructure_success(value: GenerationCompletion | SemanticCompletion) -> bool:
    if value.status == "succeeded":
        return True
    failure = value.failure
    if value.status != "failed" or failure is None or value.receipt is None:
        return False
    if failure.kind == "output_contract_conformance":
        return True
    return failure.kind == "semantic_execution" and failure.code in {
        "semantic_output_missing_or_too_large",
        "semantic_output_schema_invalid",
        "semantic_output_decode_invalid",
        "semantic_decision_contract_invalid",
    }


def evaluate_quality_gates(
    candidates: tuple[QualityCandidate, ...],
    generations: tuple[GenerationCompletion, ...],
    semantics: tuple[SemanticCompletion, ...],
    reviews: tuple[CandidateHumanReview, ...],
    thresholds: QualityThresholds,
    closed_at: datetime,
    evaluated_at: datetime,
) -> QualityGateResult:
    """Compute quality and accountability after G1/G2 accepted a closed evidence inventory."""
    if not all(isinstance(v, tuple) for v in (candidates, generations, semantics, reviews)):
        raise ValueError("Immutable quality evidence required")
    if (
        evaluated_at.tzinfo is None
        or evaluated_at.utcoffset() is None
        or closed_at.tzinfo is None
        or closed_at.utcoffset() is None
        or evaluated_at < closed_at
        or any(r.reviewed_at > evaluated_at for r in reviews)
    ):
        raise ValueError("Gate time cannot predate closed evidence or reviews")
    counts = Counter(c.case_id for c in candidates)
    if (
        len(counts) != thresholds.generation_cases
        or any(v != thresholds.candidates_per_case for v in counts.values())
        or len({(c.case_id, c.slot_ordinal) for c in candidates}) != len(candidates)
        or any(not 1 <= c.slot_ordinal <= thresholds.candidates_per_case for c in candidates)
        or len({c.evidence.candidate_id for c in candidates}) != len(candidates)
    ):
        raise ValueError("Quality gates require the complete unique candidate inventory")
    generated = {g.execution_id: g for g in generations}
    judged = {s.execution_id: s for s in semantics}
    if len(generated) != len(generations) or len(judged) != len(semantics):
        raise ValueError("Execution evidence must be unique")
    for item in candidates:
        g = generated.get(item.generation_execution_id)
        evidence = item.evidence
        if (
            g is None
            or g.status != "succeeded"
            or (g.case_id, g.slot_ordinal) != (item.case_id, item.slot_ordinal)
            or g.normalized_output != evidence.normalized_output
            or judged.get(evidence.semantic.execution_id) != evidence.semantic
            or evidence.semantic.finished_at > closed_at
        ):
            raise ValueError("Quality candidate differs from accepted execution evidence")
    if reviews:
        add_human_reviews(
            "awaiting_review",
            closed_at,
            tuple(c.evidence for c in candidates),
            (),
            reviews,
            gate_policy_version="v2",
        )
    reasons: list[GateReason] = []
    metrics: list[GateMetric] = []
    adjudications: list[SemanticAdjudication] = []

    def reject(gate: str, code: str, *refs: str) -> None:
        reasons.append(GateReason(gate, code, refs))

    def rate(name: str, numerator: int, denominator: int, minimum: float, code: str) -> None:
        value = numerator / denominator if denominator else 0.0
        metrics.append(GateMetric(name, numerator, denominator, value, minimum))
        if value < minimum:
            reject("G3", code)

    executions: tuple[GenerationCompletion | SemanticCompletion, ...] = (*generations, *semantics)
    dispatched = tuple(v for v in executions if v.provider_call_count == 1)
    definite = tuple(g for g in generations if g.raw_output and g.status != "result_unknown")
    semantic_dispatched = sum(s.provider_call_count for s in semantics)
    rate(
        "infrastructure_success_rate",
        sum(infrastructure_success(v) for v in dispatched),
        len(dispatched),
        thresholds.min_infrastructure_rate,
        "infrastructure_success_rate_below_threshold",
    )
    rate(
        "generation_contract_conformance_rate",
        sum(g.status == "succeeded" for g in definite),
        len(definite),
        thresholds.min_contract_rate,
        "generation_contract_conformance_rate_below_threshold",
    )
    rate(
        "semantic_execution_success_rate",
        sum(s.status == "succeeded" for s in semantics),
        semantic_dispatched,
        thresholds.min_semantic_rate,
        "semantic_execution_success_rate_below_threshold",
    )
    case_passes = dict.fromkeys(counts, 0)
    totals = [0] * len(SCORE_NAMES)
    for item in candidates:
        candidate = item.evidence
        candidate_reviews = tuple(r for r in reviews if r.candidate_id == candidate.candidate_id)
        effective = effective_candidate_assertions(
            candidate, candidate_reviews, closed_at, gate_policy_version="v2"
        )
        if effective.adjudication is not None:
            adjudications.append(effective.adjudication)
        present, passed, hard_passed = assertion_outcome(effective.assertions)
        if not present:
            reject("G4", "candidate_case_assertion_failed", candidate.candidate_id)
        elif passed:
            case_passes[item.case_id] += 1
        if not hard_passed:
            reject("G4", "candidate_hard_assertion_failed", candidate.candidate_id)
        scores = json.loads(candidate.semantic.normalized_output).get("scores", {})
        if set(scores) != set(SCORE_NAMES) or any(
            type(scores[k]) is not int or not 1 <= scores[k] <= 5 for k in SCORE_NAMES
        ):
            raise ValueError("Accepted semantic scores are incomplete or invalid")
        values = tuple(scores[k] for k in SCORE_NAMES)
        totals = [a + b for a, b in zip(totals, values, strict=True)]
        if any(v < minimum for v, minimum in zip(values, thresholds.minimum_scores, strict=True)):
            reject("G4", "candidate_semantic_score_below_minimum", candidate.candidate_id)
        if any(r.decision == "reject" for r in candidate_reviews):
            reject("G5", "human_review_rejected", candidate.candidate_id)
        roles = {r.role for r in candidate_reviews}
        for role in ("assessment_semantics", "safety_product"):
            if role not in roles:
                reject("G5", "human_review_incomplete", candidate.candidate_id)
    for case_id in sorted(case_passes):
        if case_passes[case_id] < thresholds.min_case_passes:
            reject("G4", "case_assertion_stability_failed", case_id)
    if sum(case_passes.values()) < thresholds.min_overall_passes:
        reject("G4", "case_assertion_overall_failed")
    if any(
        total / len(candidates) < minimum
        for total, minimum in zip(totals, thresholds.minimum_averages, strict=True)
    ):
        reject("G4", "semantic_average_below_threshold")
    if len(reviews) != thresholds.review_count:
        reject("G5", "human_review_count_incomplete")
    return QualityGateResult(
        evaluated_at,
        tuple((g, not any(r.gate == g for r in reasons)) for g in ("G3", "G4", "G5")),
        tuple(metrics),
        tuple(reasons),
        tuple(adjudications),
    )
