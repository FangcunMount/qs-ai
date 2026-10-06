"""Decode persisted review signatures without importing write-side services."""

from datetime import datetime

from qs_ai.domain.evaluation.review import CandidateHumanReview, SemanticContradictionReview
from qs_ai.domain.evaluation.review_correction import ReviewCorrection, effective_reviews


def decode_reviews(values: list[dict]) -> tuple[CandidateHumanReview, ...]:
    result = []
    for value in values:
        fields = {**value, "reviewed_at": datetime.fromisoformat(value["reviewed_at"])}
        if fields.get("semantic_review") is not None:
            fields["semantic_review"] = SemanticContradictionReview(**fields["semantic_review"])
        result.append(CandidateHumanReview(**fields))
    return tuple(result)


def encode_review(review: CandidateHumanReview) -> dict:
    from dataclasses import asdict

    return {**asdict(review), "reviewed_at": review.reviewed_at.isoformat()}


def encode_correction(entry: ReviewCorrection) -> dict:
    from dataclasses import asdict

    return {
        **asdict(entry),
        "previous_review": encode_review(entry.previous_review),
        "review": encode_review(entry.review),
    }


def decode_corrections(progress: dict) -> tuple[ReviewCorrection, ...]:
    raw = progress.get("review_corrections", [])
    if not isinstance(raw, list):
        raise ValueError("Review correction audit must be an array")
    return tuple(
        ReviewCorrection(
            **{
                **entry,
                "previous_review": decode_reviews([entry["previous_review"]])[0],
                "review": decode_reviews([entry["review"]])[0],
            }
        )
        for entry in raw
    )


def current_reviews(progress: dict, version: int) -> tuple[CandidateHumanReview, ...]:
    return effective_reviews(
        decode_reviews(progress.get("human_reviews", [])), decode_corrections(progress), version
    )
