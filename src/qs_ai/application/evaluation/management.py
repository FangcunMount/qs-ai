"""Management is delegated by an authenticated QS backend after QS governance authorization."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from qs_ai.domain.evaluation.resolution import ResultUnknownResolution


@dataclass(frozen=True)
class ManagementScope:
    run_id: UUID
    organization_id: int
    operator_user_id: int

    def __post_init__(self) -> None:
        if self.run_id.int == 0 or any(
            type(value) is not int or not 0 < value <= 2**63 - 1
            for value in (self.organization_id, self.operator_user_id)
        ):
            raise ValueError("Valid management scope required")

    @property
    def actor(self) -> str:
        return "user:" + str(self.operator_user_id)


@dataclass(frozen=True)
class EvaluationView:
    run_id: str
    version: int
    status: str
    unresolved_result_unknown_count: int
    resolutions_json: str


class EvaluationManagementStore(Protocol):
    async def get(self, scope: ManagementScope) -> EvaluationView: ...
    async def resolve(
        self,
        scope: ManagementScope,
        expected_version: int,
        value: ResultUnknownResolution,
        *,
        confirm: bool,
    ) -> EvaluationView: ...
