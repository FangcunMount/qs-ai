"""One in-flight evaluation execution; persistence must compare aggregate version."""

import re
from dataclasses import dataclass, replace
from datetime import datetime


def _identity(value: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", value) is not None


def _time(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class ExecutionCheckpoint:
    execution_id: str
    kind: str
    case_id: str
    slot_ordinal: int
    candidate_id: str
    execution_ordinal: int
    owner: str
    invocation_id: str
    phase: str
    claimed_at: datetime
    lease_expires_at: datetime
    dispatch_started_at: datetime | None = None

    def __post_init__(self) -> None:
        if not all(
            _identity(x) for x in (self.execution_id, self.case_id, self.owner, self.invocation_id)
        ):
            raise ValueError("Invalid checkpoint identity")
        if (
            type(self.slot_ordinal) is not int
            or not 1 <= self.slot_ordinal <= 5
            or type(self.execution_ordinal) is not int
            or not 1 <= self.execution_ordinal <= 2
        ):
            raise ValueError("Invalid checkpoint ordinal")
        if self.kind == "generation":
            if self.candidate_id:
                raise ValueError("Generation checkpoint cannot reference a candidate")
        elif self.kind != "semantic" or not _identity(self.candidate_id):
            raise ValueError("Semantic checkpoint requires a candidate")
        if (
            not _time(self.claimed_at)
            or not _time(self.lease_expires_at)
            or self.lease_expires_at <= self.claimed_at
        ):
            raise ValueError("Invalid checkpoint lease")
        if self.phase == "prepared":
            if self.dispatch_started_at is not None:
                raise ValueError("Prepared checkpoint cannot have dispatch time")
        elif self.phase != "dispatching" or (
            self.dispatch_started_at is None
            or not _time(self.dispatch_started_at)
            or not self.claimed_at <= self.dispatch_started_at <= self.lease_expires_at
        ):
            raise ValueError("Invalid checkpoint dispatch time")

    def mark_dispatching(self, owner: str, at: datetime) -> "ExecutionCheckpoint":
        if self.phase != "prepared" or owner.strip() != self.owner:
            raise ValueError("Prepared execution with matching owner required")
        return replace(self, phase="dispatching", dispatch_started_at=at)

    def can_release_preparation(
        self, invocation_id: str, observed_expiry: datetime, at: datetime
    ) -> bool:
        """Require the exact expired checkpoint; never release a dispatched model call."""
        if not _time(observed_expiry) or not _time(at):
            raise ValueError("Recovery times must have time zones")
        return (
            self.phase == "prepared"
            and invocation_id.strip() == self.invocation_id
            and observed_expiry == self.lease_expires_at
            and at >= self.lease_expires_at
        )
