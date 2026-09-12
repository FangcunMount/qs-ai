"""Dual-role semantic adjudication derives a view without overwriting provider evidence."""

from dataclasses import dataclass, replace
from datetime import datetime

from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.review import (
    CandidateHumanReview,
    ReviewCandidate,
    SemanticContradictionReview,
    add_human_reviews,
)


@dataclass(frozen=True)
class SemanticAdjudication:
    policy_version: str
    candidate_id: str
    execution_id: str
    output_fingerprint: str
    assertion_type: str
    assertion_ordinal: int
    original_status: str
    effective_status: str
    reviewers: tuple[str, str]


@dataclass(frozen=True)
class EffectiveCandidateAssertions:
    assertions: tuple[AssertionReceipt, ...]
    adjudication: SemanticAdjudication | None = None


def effective_candidate_assertions(
    candidate: ReviewCandidate,
    reviews: tuple[CandidateHumanReview, ...],
    closed_at: datetime,
    *,
    gate_policy_version: str,
) -> EffectiveCandidateAssertions:
    """Revalidate the signed history against its closure before deriving a judgment.

    The caller supplies only this candidate's reviews, including ordinary approvals
    and rejections. This is not a quality gate or a release approval.
    """
    if not isinstance(reviews, tuple):
        raise ValueError("Immutable candidate review history required")
    if not reviews:
        return EffectiveCandidateAssertions(candidate.assertions)
    # Validate as an accepted review batch at the original evidence closure;
    # the current Run may since have been finalized without changing this history.
    add_human_reviews(
        "awaiting_review",
        closed_at,
        (candidate,),
        (),
        reviews,
        gate_policy_version=gate_policy_version,
    )
    signed = {
        r.role: r for r in reviews if r.decision == "approve" and r.semantic_review is not None
    }
    if set(signed) != {"assessment_semantics", "safety_product"}:
        return EffectiveCandidateAssertions(candidate.assertions)
    assessment, safety = signed["assessment_semantics"], signed["safety_product"]
    a, b = assessment.semantic_review, safety.semantic_review
    assert a is not None and b is not None

    # Each excerpt/reason is independently checked; signers may cite different passages.
    def identity(r: SemanticContradictionReview) -> tuple[str, str, str, int, str]:
        return (
            r.policy_version,
            r.execution_id,
            r.output_fingerprint,
            r.assertion_ordinal,
            r.original_detail,
        )

    if identity(a) != identity(b):
        return EffectiveCandidateAssertions(candidate.assertions)
    effective = tuple(
        replace(value, status="passed")
        if (value.type, value.scope, value.ordinal, value.status, value.detail, value.evaluator)
        == (
            "forbidden_claims_absent",
            "default",
            a.assertion_ordinal,
            "failed",
            a.original_detail,
            candidate.semantic_evaluator,
        )
        else value
        for value in candidate.assertions
    )
    return EffectiveCandidateAssertions(
        effective,
        SemanticAdjudication(
            a.policy_version,
            candidate.candidate_id,
            a.execution_id,
            a.output_fingerprint,
            "forbidden_claims_absent",
            a.assertion_ordinal,
            "failed",
            "passed",
            (assessment.reviewer, safety.reviewer),
        ),
    )
