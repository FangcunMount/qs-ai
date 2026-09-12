"""Append-only candidate reviews, including evidence-bound semantic contradiction review."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.resolution import valid_id
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion

CONTRADICTION_POLICY = "semantic-contradiction-dual-review/v1"


def _text(value: str, limit: int) -> bool:
    return (
        bool(value.strip()) and len(value.encode()) <= limit and not any(c in value for c in "<>")
    )


def _aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class SemanticContradictionReview:
    policy_version: str
    execution_id: str
    output_fingerprint: str
    assertion_ordinal: int
    original_detail: str
    candidate_excerpt: str
    reason: str

    def __post_init__(self) -> None:
        if (
            self.policy_version != CONTRADICTION_POLICY
            or not valid_id(self.execution_id)
            or type(self.assertion_ordinal) is not int
            or self.assertion_ordinal < 1
            or not _text(self.original_detail, 2000)
            or not _text(self.candidate_excerpt, 1000)
            or not _text(self.reason, 1000)
        ):
            raise ValueError("Invalid semantic contradiction review")


@dataclass(frozen=True)
class CandidateHumanReview:
    candidate_id: str
    role: str
    reviewer: str
    decision: str
    reviewed_at: datetime
    reason: str
    semantic_review: SemanticContradictionReview | None = None

    def __post_init__(self) -> None:
        if (
            not valid_id(self.candidate_id)
            or not valid_id(self.reviewer)
            or self.role not in ("assessment_semantics", "safety_product")
            or self.decision not in ("approve", "reject")
            or not _aware(self.reviewed_at)
            or not _text(self.reason, 1000)
            or (
                self.semantic_review is not None
                and not isinstance(self.semantic_review, SemanticContradictionReview)
            )
        ):
            raise ValueError("Invalid candidate human review")


@dataclass(frozen=True)
class ReviewCandidate:
    """Projection of persisted generation and its accepted semantic evidence."""

    candidate_id: str
    accepted_at: datetime
    normalized_output: bytes
    semantic: SemanticCompletion
    semantic_assertions: tuple[AssertionReceipt, ...]

    def __post_init__(self) -> None:
        if (
            not _aware(self.accepted_at)
            or self.semantic.status != "succeeded"
            or self.candidate_id != self.semantic.candidate_id
            or self.accepted_at > self.semantic.started_at
            or not isinstance(self.normalized_output, bytes)
            or not self.normalized_output
            or "sha256:" + hashlib.sha256(self.normalized_output).hexdigest()
            != self.semantic.candidate_output_fingerprint
            or not isinstance(self.semantic_assertions, tuple)
            or not self.semantic_assertions
            or any(not isinstance(a, AssertionReceipt) for a in self.semantic_assertions)
        ):
            raise ValueError("Review candidate lacks accepted evidence")


def _validate_semantic_review(
    value: CandidateHumanReview, candidate: ReviewCandidate, gate_policy_version: str
) -> None:
    review = value.semantic_review
    if review is None:
        return
    semantic = candidate.semantic
    if (
        gate_policy_version != "v2"
        or value.decision != "approve"
        or review.execution_id != semantic.execution_id
        or review.output_fingerprint != semantic.output_fingerprint
        or review.candidate_excerpt.encode() not in candidate.normalized_output
    ):
        raise ValueError("Semantic review does not bind accepted evidence")
    # The assertion must exist in both the candidate projection and the original
    # provider output. A review never changes either of these evidence records.
    expected = (
        "forbidden_claims_absent",
        "default",
        review.assertion_ordinal,
        "failed",
        review.original_detail,
    )
    original = json.loads(semantic.normalized_output)
    matches = [
        a
        for a in candidate.semantic_assertions
        if (a.type, a.scope, a.ordinal, a.status, a.detail) == expected
    ]
    decisions = original.get("decisions", [])
    if (
        len(matches) != 1
        or sum(
            (a.get("type"), a.get("scope"), a.get("ordinal"), a.get("status"), a.get("detail"))
            == expected
            for a in decisions
        )
        != 1
    ):
        raise ValueError("Original failed forbidden-claims assertion required")


def add_human_reviews(
    status: str,
    closed_at: datetime,
    candidates: tuple[ReviewCandidate, ...],
    prior: tuple[CandidateHumanReview, ...],
    values: tuple[CandidateHumanReview, ...],
    *,
    gate_policy_version: str,
) -> tuple[CandidateHumanReview, ...]:
    """Validate the complete proposed history before returning an immutable append.

    The application supplies authenticated reviewers and server timestamps;
    persistence must load the full history and commit once under Run CAS.
    Passing this function does not approve the Run or publish a release.
    """
    if (
        status != "awaiting_review"
        or not _aware(closed_at)
        or not all(isinstance(v, tuple) for v in (candidates, prior, values))
        or not candidates
        or not values
    ):
        raise ValueError("Closed reviewable Run and nonempty review batch required")
    targets = {c.candidate_id: c for c in candidates}
    if len(targets) != len(candidates) or any(
        c.semantic.finished_at > closed_at for c in candidates
    ):
        raise ValueError("Candidate evidence differs from closed Run")
    roles: set[tuple[str, str]] = set()
    reviewers: set[tuple[str, str]] = set()
    result = (*prior, *values)
    for review in result:
        candidate = targets.get(review.candidate_id)
        if candidate is None or review.reviewed_at < closed_at:
            raise ValueError("Review target or timestamp outside closed evidence")
        role_key = (review.candidate_id, review.role)
        reviewer_key = (review.candidate_id, review.reviewer)
        if role_key in roles or reviewer_key in reviewers:
            raise ValueError("Candidate requires unique roles and distinct reviewers")
        _validate_semantic_review(review, candidate, gate_policy_version)
        roles.add(role_key)
        reviewers.add(reviewer_key)
    return result
