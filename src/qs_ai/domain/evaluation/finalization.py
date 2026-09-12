"""Final review rules after frozen identity and collection evidence have been validated."""

from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.quality_gates import QualityGateResult
from qs_ai.domain.evaluation.resolution import valid_id


@dataclass(frozen=True)
class FinalReview:
    actor: str
    reason: str
    finalized_at: datetime
    passed: bool

    def __post_init__(self) -> None:
        if (
            not valid_id(self.actor)
            or not self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
            or self.finalized_at.tzinfo is None
            or self.finalized_at.utcoffset() is None
            or type(self.passed) is not bool
        ):
            raise ValueError("Invalid final review audit")

    @property
    def status(self) -> str:
        return "approved" if self.passed else "rejected"


def finalize_review(quality: QualityGateResult, actor: str, reason: str) -> FinalReview:
    """Incomplete review is pending work, even when other gates already fail."""
    gates = dict(quality.gate_passes)
    if (
        len(quality.gate_passes) != 3
        or set(gates) != {"G3", "G4", "G5"}
        or any(type(value) is not bool for value in gates.values())
        or any(
            r.code in {"human_review_incomplete", "human_review_count_incomplete"}
            for r in quality.reasons
        )
    ):
        raise ValueError("Finalization requires complete validated human review")
    return FinalReview(actor, reason, quality.evaluated_at, all(gates.values()))
