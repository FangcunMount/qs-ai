"""QS-authorized participant execution administration; no local user registry."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.service import external_id


@dataclass(frozen=True)
class ParticipantCapacityQuery:
    scope: DraftScope
    subject_id: str = ""
    assessment_id: str = ""

    def __post_init__(self) -> None:
        if len(self.subject_id) > 128 or any(ord(c) < 32 for c in self.subject_id):
            raise ValueError("Invalid participant subject filter")
        if self.assessment_id and not external_id(self.assessment_id):
            raise ValueError("Invalid assessment filter")


@dataclass(frozen=True)
class ParticipantCapacityUsage:
    identity: str
    daily_reserved: int
    daily_remaining: int
    active: int
    active_remaining: int


@dataclass(frozen=True)
class ParticipantReservation:
    run_id: str
    session_id: str
    subject_id: str
    assessment_ids: tuple[str, ...]
    budget_day: str
    reserved_at: str
    active: bool
    acquired_at: str


@dataclass(frozen=True)
class ParticipantCapacitySnapshot:
    organization_id: int
    budget_day: str
    policy: ParticipantCapacityPolicy
    organization: ParticipantCapacityUsage
    subject: ParticipantCapacityUsage | None
    assessment: ParticipantCapacityUsage | None
    daily_reservations: tuple[ParticipantReservation, ...]
    active_reservations: tuple[ParticipantReservation, ...]
    daily_truncated: bool
    active_truncated: bool


class ParticipantCapacityReader(Protocol):
    async def get(
        self, query: ParticipantCapacityQuery, at: datetime
    ) -> ParticipantCapacitySnapshot: ...
