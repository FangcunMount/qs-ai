"""One explicitly authorized unknown-result decision; first audit time belongs to the winner."""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
)

_MAGIC = b"qs-ai-resolution-state/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate original resolution fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class ResolutionStateOperation:
    scope: ManagementScope
    operation_id: UUID
    expected_version: int
    value: ResultUnknownResolution = field(repr=False)
    confirm: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ManagementScope)
            or not isinstance(self.operation_id, UUID)
            or not self.operation_id.int
            or type(self.expected_version) is not int
            or not 0 < self.expected_version < 2**63 - 1
            or not isinstance(self.value, ResultUnknownResolution)
            or self.value.actor != self.scope.actor
            or self.confirm is not True
        ):
            raise ValueError("Explicit scoped original resolution required")

    def _value(self, *, clock: bool) -> dict[str, Any]:
        value = asdict(self.value)
        value.pop("resolved_at")
        if clock:
            value["resolved_at"] = self.value.resolved_at.isoformat(timespec="microseconds")
        return dict(
            run_id=str(self.scope.run_id),
            organization_id=self.scope.organization_id,
            operator_user_id=self.scope.operator_user_id,
            operation_id=str(self.operation_id),
            expected_version=self.expected_version,
            value=value,
            confirm=self.confirm,
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.scope.run_id),
            "governance/resolve/" + str(self.operation_id),
            self.expected_version,
            hashlib.sha256(
                b"qs-ai-resolution-input/v1\0" + _json(self._value(clock=False))
            ).digest(),
        )

    def encode(self, snapshot_digest: bytes) -> bytes:
        if type(snapshot_digest) is not bytes or len(snapshot_digest) != 32:
            raise ValueError("Exact original resolution source required")
        result = _MAGIC + _json(
            dict(operation=self._value(clock=True), snapshot_digest=snapshot_digest.hex())
        )
        if len(result) > 8192:
            raise ValueError("Original resolution facts exceed bounds")
        return result

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["ResolutionStateOperation", bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded resolution facts required")
        try:
            obj = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(obj, dict) or set(obj) != {"operation", "snapshot_digest"}:
                raise ValueError
            value = obj["operation"]
            if not isinstance(value, dict) or set(value) != {
                "run_id",
                "organization_id",
                "operator_user_id",
                "operation_id",
                "expected_version",
                "value",
                "confirm",
            }:
                raise ValueError
            audit = value["value"]
            if not isinstance(audit, dict) or set(audit) != {
                "execution_id",
                "decision",
                "actor",
                "reason",
                "acknowledged_duplicate_call_and_cost_risk",
                "resolved_at",
            }:
                raise ValueError
            operation = cls(
                ManagementScope(
                    UUID(value["run_id"]), value["organization_id"], value["operator_user_id"]
                ),
                UUID(value["operation_id"]),
                value["expected_version"],
                ResultUnknownResolution(
                    **{**audit, "resolved_at": datetime.fromisoformat(audit["resolved_at"])}
                ),
                value["confirm"],
            )
            digest = bytes.fromhex(obj["snapshot_digest"])
            if (
                operation.intent != facts.intent
                or operation.encode(digest) != raw
                or operation.value.resolved_at != facts.at
            ):
                raise ValueError
            return operation, digest
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original resolution facts") from None
