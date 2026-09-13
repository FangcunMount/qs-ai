"""Cancel future work without erasing evidence or treating dispatched calls as undone."""

import re
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint


@dataclass(frozen=True)
class CancellationDecision:
    actor: str
    reason: str
    discard: bool
    canceled_at: datetime

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"user:[1-9][0-9]*", self.actor)
            or not self.reason.strip()
            or self.reason != self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
            or type(self.discard) is not bool
            or self.canceled_at.utcoffset() is None
        ):
            raise ValueError("Cancellation requires an operator, reason, decision and time")

    def cause(self, status: str, unresolved: int, checkpoint: ExecutionCheckpoint | None) -> str:
        if (
            status not in ("requested", "collecting", "blocked", "awaiting_review")
            or type(unresolved) is not int
            or unresolved != 0
            or self.discard != (status == "awaiting_review")
        ):
            raise ValueError("Run cannot be canceled; resolve unknown calls first")
        if checkpoint is not None and (
            checkpoint.phase != "prepared"
            or status != "collecting"
            or self.canceled_at < checkpoint.claimed_at
        ):
            raise ValueError("Dispatched execution must finish or recover before cancellation")
        return "operator_discarded" if self.discard else "operator_canceled"
