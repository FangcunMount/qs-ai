"""Participant provider-call limits, independent from evaluation-run budgets."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ParticipantCapacityPolicy:
    daily_org: int = 500
    daily_user: int = 5
    daily_assessment: int = 3
    active_org: int = 10
    active_user: int = 2
    active_assessment: int = 1

    def __post_init__(self) -> None:
        if any(
            type(v) is not int or v < 1
            for v in (
                self.daily_org,
                self.daily_user,
                self.daily_assessment,
                self.active_org,
                self.active_user,
                self.active_assessment,
            )
        ):
            raise ValueError("Positive participant capacity limits required")


DEFAULT_PARTICIPANT_CAPACITY = ParticipantCapacityPolicy()
