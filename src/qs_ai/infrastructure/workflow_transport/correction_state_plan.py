"""Complete immutable correction input and first audit clock, separate from execution."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
)

_MAGIC = b"qs-ai-correction-state/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate correction fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class CorrectionStateOperation:
    scope: ManagementScope
    expected_version: int
    command_id: str
    previous_review_fingerprint: str
    candidate_output_fingerprint: str
    value: CandidateHumanReview = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ManagementScope)
            or type(self.expected_version) is not int
            or not 0 < self.expected_version < 2**63 - 1
            or type(self.command_id) is not str
            or str(UUID(self.command_id)) != self.command_id
            or not UUID(self.command_id).int
            or not isinstance(self.value, CandidateHumanReview)
            or self.value.reviewer != self.scope.actor
            or self.value.semantic_review is not None
            or any(
                type(v) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", v) is None
                for v in (self.previous_review_fingerprint, self.candidate_output_fingerprint)
            )
        ):
            raise ValueError("Original authorized correction identity required")

    def _fields(self, *, clock: bool) -> dict[str, Any]:
        value = dict(
            candidate_id=self.value.candidate_id,
            role=self.value.role,
            reviewer=self.value.reviewer,
            decision=self.value.decision,
            reason=self.value.reason,
        )
        if clock:
            value["reviewed_at"] = self.value.reviewed_at.isoformat(timespec="microseconds")
        return dict(
            run_id=str(self.scope.run_id),
            organization_id=self.scope.organization_id,
            operator_user_id=self.scope.operator_user_id,
            expected_version=self.expected_version,
            command_id=self.command_id,
            previous_review_fingerprint=self.previous_review_fingerprint,
            candidate_output_fingerprint=self.candidate_output_fingerprint,
            review=value,
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.scope.run_id),
            "governance/correct-review/" + self.command_id,
            self.expected_version,
            hashlib.sha256(
                b"qs-ai-correction-input/v1\0" + _json(self._fields(clock=False))
            ).digest(),
        )

    def encode(self, snapshot_digest: bytes) -> bytes:
        if type(snapshot_digest) is not bytes or len(snapshot_digest) != 32:
            raise ValueError("Original correction source snapshot required")
        raw = _MAGIC + _json(
            dict(operation=self._fields(clock=True), snapshot_digest=snapshot_digest.hex())
        )
        if len(raw) > 8192:
            raise ValueError("Frozen correction input exceeds bounds")
        return raw

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["CorrectionStateOperation", bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded correction facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {"operation", "snapshot_digest"}:
                raise ValueError
            operation = value["operation"]
            if not isinstance(operation, dict) or set(operation) != {
                "run_id",
                "organization_id",
                "operator_user_id",
                "expected_version",
                "command_id",
                "previous_review_fingerprint",
                "candidate_output_fingerprint",
                "review",
            }:
                raise ValueError
            review = operation["review"]
            if not isinstance(review, dict) or set(review) != {
                "candidate_id",
                "role",
                "reviewer",
                "decision",
                "reason",
                "reviewed_at",
            }:
                raise ValueError
            result = cls(
                ManagementScope(
                    UUID(operation["run_id"]),
                    operation["organization_id"],
                    operation["operator_user_id"],
                ),
                operation["expected_version"],
                operation["command_id"],
                operation["previous_review_fingerprint"],
                operation["candidate_output_fingerprint"],
                CandidateHumanReview(
                    review["candidate_id"],
                    review["role"],
                    review["reviewer"],
                    review["decision"],
                    datetime.fromisoformat(review["reviewed_at"]),
                    review["reason"],
                ),
            )
            digest = bytes.fromhex(value["snapshot_digest"])
            if result.intent != facts.intent or result.encode(digest) != raw:
                raise ValueError
            return result, digest
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen correction facts") from None
