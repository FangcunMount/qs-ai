"""Bounded audit views; a list entry is not proof that a candidate passed review."""

import re
from dataclasses import dataclass


def validate_candidate_query(candidate_id: str, expected_version: int) -> None:
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", candidate_id)
        or type(expected_version) is not int
        or expected_version < 1
    ):
        raise ValueError("Candidate identity and explicit Run version required")


@dataclass(frozen=True)
class CandidateSummary:
    candidate_id: str
    case_id: str
    slot_ordinal: int


@dataclass(frozen=True)
class CandidateIndex:
    run_id: str
    version: int
    candidates: tuple[CandidateSummary, ...]


@dataclass(frozen=True)
class CandidateEvidence:
    run_id: str
    version: int
    candidate_id: str
    # Original normalized bytes are retained for exact excerpt/fingerprint comparison.
    normalized_output: bytes
    semantic_output: bytes
    # Frozen release, receipts, assertions, semantic result and this candidate's reviews.
    evidence_json: str
