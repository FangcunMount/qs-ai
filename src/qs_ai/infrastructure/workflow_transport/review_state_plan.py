"""Review input binding and first clocks; no SQL, signing, execution or recovery.

The host retains the immutable authorized RPC input, as command executors retain
original authenticated input. The durable plan binds its digest and first clocks.
A checker needs only witness evidence; it never reconstructs or reruns reviews.
"""

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from typing import Any
from uuid import UUID

from reliable_messaging.transaction import FrozenPlan

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.review import CandidateHumanReview
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

_MAGIC = b"qs-ai-review-state/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate frozen review fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class ReviewStateInput:
    scope: ManagementScope
    operation_id: UUID
    expected_version: int
    values: tuple[CandidateHumanReview, ...] = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ManagementScope)
            or not isinstance(self.operation_id, UUID)
            or not self.operation_id.int
            or type(self.expected_version) is not int
            or not 0 < self.expected_version < 2**63 - 1
            or type(self.values) is not tuple
            or not 1 <= len(self.values) <= 35
            or any(not isinstance(v, CandidateHumanReview) for v in self.values)
            or any(v.reviewer != self.scope.actor for v in self.values)
            or len({v.role for v in self.values}) != 1
            or len({v.candidate_id for v in self.values}) != len(self.values)
        ):
            raise ValueError("Original scoped single-role review input required")

    @property
    def input_digest(self) -> bytes:
        values = []
        for value in self.values:
            item = asdict(value)
            item.pop("reviewed_at")
            values.append(item)
        return hashlib.sha256(
            b"qs-ai-review-input/v1\0"
            + _json(
                dict(
                    run_id=str(self.scope.run_id),
                    organization_id=self.scope.organization_id,
                    operator_user_id=self.scope.operator_user_id,
                    operation_id=str(self.operation_id),
                    expected_version=self.expected_version,
                    values=values,
                )
            )
        ).digest()

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.scope.run_id),
            f"governance/review/{self.operation_id}",
            self.expected_version,
            self.input_digest,
        )


@dataclass(frozen=True)
class ReviewPlanFacts:
    snapshot_digest: bytes = field(repr=False)
    input_digest: bytes = field(repr=False)
    reviewed_at: tuple[datetime, ...] = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.snapshot_digest) is not bytes
            or len(self.snapshot_digest) != 32
            or type(self.input_digest) is not bytes
            or len(self.input_digest) != 32
            or type(self.reviewed_at) is not tuple
            or not 1 <= len(self.reviewed_at) <= 35
            or any(not isinstance(v, datetime) or v.utcoffset() is None for v in self.reviewed_at)
        ):
            raise ValueError("Bounded exact review input, source and first clocks required")

    def encode(self) -> bytes:
        raw = _MAGIC + _json(
            dict(
                snapshot_digest=self.snapshot_digest.hex(),
                input_digest=self.input_digest.hex(),
                reviewed_at=[v.isoformat(timespec="microseconds") for v in self.reviewed_at],
            )
        )
        if len(raw) > 8192:
            raise ValueError("Frozen review facts exceed bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "ReviewPlanFacts":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded review facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if (
                not isinstance(value, dict)
                or set(value) != {"snapshot_digest", "input_digest", "reviewed_at"}
                or not isinstance(value["reviewed_at"], list)
            ):
                raise ValueError
            result = cls(
                bytes.fromhex(value["snapshot_digest"]),
                bytes.fromhex(value["input_digest"]),
                tuple(datetime.fromisoformat(v) for v in value["reviewed_at"]),
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen review facts") from None

    def values_for(self, incoming: ReviewStateInput) -> tuple[CandidateHumanReview, ...]:
        if self.input_digest != incoming.input_digest or len(self.reviewed_at) != len(
            incoming.values
        ):
            raise ValueError("Frozen original review input differs")
        return tuple(
            replace(value, reviewed_at=at)
            for value, at in zip(
                incoming.values,
                self.reviewed_at,
                strict=True,
            )
        )


def read_review_state_plan(
    incoming: ReviewStateInput, winner: FrozenPlan
) -> tuple[
    EvaluationStateFacts,
    ReviewPlanFacts,
    FrozenArchive,
]:
    facts, archived = read_evaluation_state_plan(incoming.intent, winner)
    original = ReviewPlanFacts.decode(facts.business_context)
    original.values_for(incoming)
    state = pb.MessagingBody.FromString(archived.body).evaluation_state
    if (
        winner.operation_id != str(incoming.operation_id)
        or state.version != incoming.expected_version + 1
        or state.status != "awaiting_review"
    ):
        raise ValueError("Exact original review target and single CAS required")
    return facts, original, archived
