"""Typed organization quotas; deployment ceilings are never writable by an organization."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.prompt_drafts import DraftScope


@dataclass(frozen=True)
class QuotaValues:
    participant: ParticipantCapacityPolicy
    evaluation: EvaluationCapacityPolicy

    @classmethod
    def parse(cls, raw: dict[str, Any]) -> "QuotaValues":
        if not isinstance(raw, dict) or set(raw) != {"participant", "evaluation"}:
            raise ValueError("Complete typed quota values required")
        if not all(isinstance(raw[k], dict) for k in raw):
            raise ValueError("Quota sections must be objects")
        expected = asdict(cls(ParticipantCapacityPolicy(), EvaluationCapacityPolicy()))
        if any(set(raw[k]) != set(expected[k]) for k in expected):
            raise ValueError("Complete quota fields required")
        return cls(
            ParticipantCapacityPolicy(**raw["participant"]),
            EvaluationCapacityPolicy(**raw["evaluation"]),
        )

    def bounded(self, ceiling: "QuotaValues") -> tuple["QuotaValues", tuple[str, ...]]:
        values, limits = asdict(self), asdict(ceiling)
        constrained = []
        for section, fields in values.items():
            for key, value in fields.items():
                if value > limits[section][key]:
                    fields[key] = limits[section][key]
                    constrained.append(f"{section}.{key}")
        return self.parse(values), tuple(constrained)

    def validate_write(self, ceiling: "QuotaValues") -> None:
        if self.bounded(ceiling)[1]:
            raise ValueError("Organization quota exceeds deployment ceiling")


@dataclass(frozen=True)
class QuotaBaseline:
    defaults: QuotaValues
    ceilings: QuotaValues

    def __post_init__(self) -> None:
        self.defaults.validate_write(self.ceilings)

    def resolve(self, configured: QuotaValues | None, revision: int) -> dict[str, Any]:
        if type(revision) is not int or revision < 0 or (configured is None) != (revision == 0):
            raise ValueError("Invalid quota pointer")
        effective, constrained = (configured or self.defaults).bounded(self.ceilings)
        return {
            "revision": revision,
            "source": "organization" if configured else "deployment_default",
            "defaults": asdict(self.defaults),
            "ceilings": asdict(self.ceilings),
            "configured": asdict(configured) if configured else None,
            "effective": asdict(effective),
            "constrained_fields": list(constrained),
            "configuration_status": [
                {
                    "category": "organization_quotas",
                    "source": "mysql" if configured else "deployment_default",
                    "version": revision,
                    "applies_to": "next_admission_or_slot",
                },
                {"category": "quota_ceilings", "source": "deployment", "applies_to": "restart"},
                {
                    "category": "execution_assets",
                    "source": "publication_snapshot",
                    "applies_to": "new_accepted_task",
                },
            ],
        }


def default_baseline() -> QuotaBaseline:
    values = QuotaValues(ParticipantCapacityPolicy(), EvaluationCapacityPolicy())
    return QuotaBaseline(values, values)


class QuotaStore(Protocol):
    async def get(self, scope: DraftScope) -> dict[str, Any]: ...
    async def receipt(self, scope: DraftScope, command_id: UUID) -> dict[str, Any]: ...
    async def history(self, scope: DraftScope, before_revision: int = 0) -> dict[str, Any]: ...
    async def apply(
        self,
        scope: DraftScope,
        command_id: UUID,
        expected_revision: int,
        reason: str,
        at: datetime,
        *,
        values: QuotaValues | None = None,
        target_revision: int | None = None,
    ) -> dict[str, Any]: ...
