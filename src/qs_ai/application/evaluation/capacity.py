"""Organization admission policy; daily reservations never imply provider billing."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from qs_ai.application.governance.prompt_drafts import DraftScope


class CapacityExceeded(Exception):
    """A configured daily or active execution limit prevents admission."""


@dataclass(frozen=True)
class EvaluationCapacityPolicy:
    daily_provider_calls: int = 1024
    max_active_runs: int = 1

    def __post_init__(self) -> None:
        if any(
            type(v) is not int or v < 1 for v in (self.daily_provider_calls, self.max_active_runs)
        ):
            raise ValueError("Positive evaluation capacity limits required")


@dataclass(frozen=True)
class EvaluationCapacityReservation:
    run_id: str
    provider_calls: int
    requested_by: str
    reserved_at: str


@dataclass(frozen=True)
class EvaluationCapacitySnapshot:
    organization_id: int
    budget_day: str
    daily_provider_calls: int
    reserved_provider_calls: int
    remaining_provider_calls: int
    full_run_provider_calls: int
    remaining_full_runs: int
    max_active_runs: int
    active_runs: int
    reservation_count: int
    reservations: tuple[EvaluationCapacityReservation, ...]
    reservations_truncated: bool


class EvaluationCapacityReader(Protocol):
    async def get(self, scope: DraftScope, at: datetime) -> EvaluationCapacitySnapshot: ...
