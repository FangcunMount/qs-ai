"""Original signatures remain immutable; corrections form an evidence-bound audit chain."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from uuid import UUID

from qs_ai.domain.evaluation.review import CandidateHumanReview, ReviewCandidate, add_human_reviews


def review_fingerprint(review: CandidateHumanReview) -> str:
    value = {**asdict(review), "reviewed_at": review.reviewed_at.isoformat()}
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class ReviewCorrection:
    command_id: str
    source_version: int
    version: int
    previous_review_fingerprint: str
    candidate_output_fingerprint: str
    previous_review: CandidateHumanReview
    review: CandidateHumanReview

    def __post_init__(self) -> None:
        if (
            str(UUID(self.command_id)) != self.command_id
            or UUID(self.command_id).int == 0
            or type(self.source_version) is not int
            or self.source_version < 1
            or type(self.version) is not int
            or self.version != self.source_version + 1
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", self.candidate_output_fingerprint)
            or self.previous_review_fingerprint != review_fingerprint(self.previous_review)
            or (self.review.candidate_id, self.review.role, self.review.reviewer)
            != (
                self.previous_review.candidate_id,
                self.previous_review.role,
                self.previous_review.reviewer,
            )
            or self.review.reviewed_at < self.previous_review.reviewed_at
            or self.review.semantic_review is not None
            or self.previous_review.semantic_review is not None
        ):
            raise ValueError("Invalid review correction audit")


def effective_reviews(
    original: tuple[CandidateHumanReview, ...],
    corrections: tuple[ReviewCorrection, ...],
    version: int,
) -> tuple[CandidateHumanReview, ...]:
    if len(corrections) > 210:
        raise ValueError("Review corrections exceed audit bound")
    result = list(original)
    seen: set[str] = set()
    counts: dict[tuple[str, str], int] = {}
    last_version = 0
    for entry in corrections:
        key = (entry.review.candidate_id, entry.review.role)
        indices = [i for i, r in enumerate(result) if (r.candidate_id, r.role) == key]
        if (
            len(indices) != 1
            or result[indices[0]] != entry.previous_review
            or entry.command_id in seen
            or entry.source_version < last_version
            or entry.version > version
        ):
            raise ValueError("Review correction chain differs from original signatures")
        counts[key] = counts.get(key, 0) + 1
        if counts[key] > 3:
            raise ValueError("Review correction limit reached")
        result[indices[0]] = entry.review
        seen.add(entry.command_id)
        last_version = entry.version
    return tuple(result)


def validate_corrections(
    original: tuple[CandidateHumanReview, ...],
    corrections: tuple[ReviewCorrection, ...],
    candidates: tuple[ReviewCandidate, ...],
    closed_at: datetime,
    version: int,
    at: datetime,
    gate_policy_version: str,
) -> tuple[CandidateHumanReview, ...]:
    targets = {c.candidate_id: c for c in candidates}
    # Validate original signatures as well as each intermediate correction, so a
    # later approval cannot conceal invalid evidence or a forged earlier audit.
    if original:
        add_human_reviews(
            "awaiting_review",
            closed_at,
            candidates,
            (),
            original,
            gate_policy_version=gate_policy_version,
        )
    for i, entry in enumerate(corrections):
        candidate = targets.get(entry.review.candidate_id)
        if (
            candidate is None
            or entry.candidate_output_fingerprint != candidate.semantic.candidate_output_fingerprint
            or entry.review.reviewed_at > at
        ):
            raise ValueError("Review correction does not bind frozen candidate evidence")
        values = effective_reviews(original, corrections[: i + 1], version)
        add_human_reviews(
            "awaiting_review",
            closed_at,
            candidates,
            (),
            values,
            gate_policy_version=gate_policy_version,
        )
    return effective_reviews(original, corrections, version)
