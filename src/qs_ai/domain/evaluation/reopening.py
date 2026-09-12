"""Audited semantic review rounds; no provider evidence is replaced or re-executed."""

from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.adjudication import effective_candidate_assertions
from qs_ai.domain.evaluation.finalization import FinalReview
from qs_ai.domain.evaluation.quality_gates import QualityGateResult
from qs_ai.domain.evaluation.review import CandidateHumanReview, ReviewCandidate, add_human_reviews


@dataclass(frozen=True)
class ReviewReopening:
    """The complete preceding review round, plus the explicitly reopened candidates."""

    previous_reviews: tuple[CandidateHumanReview, ...]
    previous_gate: QualityGateResult
    candidate_ids: tuple[str, ...]
    actor: str
    reason: str
    reopened_at: datetime

    def __post_init__(self) -> None:
        FinalReview(self.actor, self.reason, self.reopened_at, False)
        if (
            not isinstance(self.previous_reviews, tuple)
            or not self.previous_reviews
            or not isinstance(self.candidate_ids, tuple)
            or not self.candidate_ids
            or len(set(self.candidate_ids)) != len(self.candidate_ids)
            or not set(self.candidate_ids) <= {r.candidate_id for r in self.previous_reviews}
            or self.previous_gate.evaluated_at.tzinfo is None
            or self.previous_gate.evaluated_at.utcoffset() is None
            or self.reopened_at < self.previous_gate.evaluated_at
        ):
            raise ValueError("Invalid immutable review reopening audit")

    @property
    def retained_reviews(self) -> tuple[CandidateHumanReview, ...]:
        return tuple(r for r in self.previous_reviews if r.candidate_id not in self.candidate_ids)


def eligible_candidate_ids(
    candidates: tuple[ReviewCandidate, ...],
    reviews: tuple[CandidateHumanReview, ...],
    gate: QualityGateResult,
    closed_at: datetime,
    *,
    status: str,
    gate_policy_version: str,
    reopening_count: int,
) -> tuple[str, ...]:
    """Callers must first rederive G1/G2 and this final gate from original evidence."""
    if (
        type(reopening_count) is not int
        or not 0 <= reopening_count < 3
        or status != "rejected"
        or gate_policy_version != "v2"
        or gate.gate_passes != (("G3", True), ("G4", False), ("G5", True))
        or not gate.reasons
        or any(r.gate != "G4" or r.code != "candidate_hard_assertion_failed" for r in gate.reasons)
    ):
        return ()
    if not isinstance(candidates, tuple) or not isinstance(reviews, tuple):
        raise ValueError("Immutable reopening evidence required")
    # Validate signatures again before using any derived semantic judgment.
    add_human_reviews(
        "awaiting_review",
        closed_at,
        candidates,
        (),
        reviews,
        gate_policy_version=gate_policy_version,
    )
    if len(reviews) != len(candidates) * 2 or any(
        r.reviewed_at > gate.evaluated_at for r in reviews
    ):
        raise ValueError("Final gate requires complete preceding reviews")
    ids = []
    for candidate in candidates:
        prior = tuple(r for r in reviews if r.candidate_id == candidate.candidate_id)
        effective = effective_candidate_assertions(
            candidate, prior, closed_at, gate_policy_version=gate_policy_version
        )
        failed = False
        for assertion in effective.assertions:
            if not assertion.hard or assertion.status == "passed":
                continue
            if (
                assertion.status != "failed"
                or assertion.type != "forbidden_claims_absent"
                or assertion.scope != "default"
                or assertion.evaluator != candidate.semantic_evaluator
            ):
                return ()
            # The failed judgment must be part of the accepted semantic result.
            if assertion not in candidate.semantic_assertions:
                return ()
            failed = True
        if failed:
            ids.append(candidate.candidate_id)
    return tuple(ids)


def reopen_review(
    candidates: tuple[ReviewCandidate, ...],
    reviews: tuple[CandidateHumanReview, ...],
    gate: QualityGateResult,
    closed_at: datetime,
    actor: str,
    reason: str,
    at: datetime,
    *,
    status: str,
    gate_policy_version: str,
    reopening_count: int,
) -> ReviewReopening:
    ids = eligible_candidate_ids(
        candidates,
        reviews,
        gate,
        closed_at,
        status=status,
        gate_policy_version=gate_policy_version,
        reopening_count=reopening_count,
    )
    # Reuse the same bounded actor/reason/aware-time contract as final review.
    FinalReview(actor, reason, at, False)
    if not ids or at < gate.evaluated_at:
        raise ValueError("Run is not eligible for audited semantic review reopening")
    return ReviewReopening(reviews, gate, ids, actor, reason, at)


def validate_reopened_reviews(
    reopening: ReviewReopening, current: tuple[CandidateHumanReview, ...]
) -> None:
    """New signatures must follow reopening; unaffected signatures remain unchanged."""
    if not isinstance(current, tuple):
        raise ValueError("Immutable current review history required")
    if any(
        r.candidate_id in reopening.candidate_ids and r.reviewed_at < reopening.reopened_at
        for r in current
    ):
        raise ValueError("New signature predates review reopening")
    if any(r not in current for r in reopening.retained_reviews):
        raise ValueError("Unaffected review was changed or removed")


def validate_reopening_history(
    history: tuple[ReviewReopening, ...],
    current: tuple[CandidateHumanReview, ...],
    candidates: tuple[ReviewCandidate, ...],
    closed_at: datetime,
    *,
    gate_policy_version: str,
) -> None:
    """Recheck each supplied, rederived gate's reopening and preserve signatures across rounds.

    Storage must additionally rederive every previous_gate from its frozen provider
    evidence and bind each record to Run versions and the transition boundaries.
    """
    if not isinstance(history, tuple) or len(history) > 3:
        raise ValueError("At most three immutable reopening rounds are permitted")
    for index, entry in enumerate(history):
        expected = reopen_review(
            candidates,
            entry.previous_reviews,
            entry.previous_gate,
            closed_at,
            entry.actor,
            entry.reason,
            entry.reopened_at,
            status="rejected",
            gate_policy_version=gate_policy_version,
            reopening_count=index,
        )
        if expected != entry:
            raise ValueError("Reopening candidates differ from validated semantic evidence")
        if index:
            previous = history[index - 1]
            if entry.previous_gate.evaluated_at < previous.reopened_at:
                raise ValueError("Previous finalization predates its review round")
            validate_reopened_reviews(previous, entry.previous_reviews)
    if history:
        validate_reopened_reviews(history[-1], current)
