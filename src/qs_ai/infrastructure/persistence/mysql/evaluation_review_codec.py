"""Decode persisted review signatures without importing write-side services."""

from datetime import datetime

from qs_ai.domain.evaluation.review import CandidateHumanReview, SemanticContradictionReview


def decode_reviews(values: list[dict]) -> tuple[CandidateHumanReview, ...]:
    result = []
    for value in values:
        fields = {**value, "reviewed_at": datetime.fromisoformat(value["reviewed_at"])}
        if fields.get("semantic_review") is not None:
            fields["semantic_review"] = SemanticContradictionReview(**fields["semantic_review"])
        result.append(CandidateHumanReview(**fields))
    return tuple(result)
