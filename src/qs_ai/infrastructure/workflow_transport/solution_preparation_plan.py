"""Stable original Solution request and bounded first preparation facts; no database I/O."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.solution_assets import PreparationIDs
from qs_ai.infrastructure.persistence.mysql.solution_preparation import PreparedAssetPlan
from qs_ai.infrastructure.persistence.mysql.solutions import solution_request
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

_MAGIC = b"qs-ai-solution-preparation/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate first preparation fact")
        value[key] = item
    return value


def preparation_target_digest(raw: str) -> bytes:
    return hashlib.sha256(b"qs-ai-solution-preparation-target/v1\0" + raw.encode()).digest()


@dataclass(frozen=True)
class SolutionPreparationInput:
    scope: DraftScope
    solution_id: UUID
    command: PrepareSolution = field(repr=False)
    identities: PreparationIDs = field(init=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, DraftScope)
            or not isinstance(self.solution_id, UUID)
            or not self.solution_id.int
            or not isinstance(self.command, PrepareSolution)
        ):
            raise ValueError("Original authorized solution preparation required")
        object.__setattr__(
            self, "identities", PreparationIDs.freeze(self.solution_id, self.command.command_id)
        )

    @property
    def request_text(self) -> str:
        return solution_request(self.scope, self.solution_id, self.command)

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.identities.run_id),
            "solution/prepare/" + str(self.command.command_id),
            0,
            hashlib.sha256(
                b"qs-ai-solution-preparation-input/v1\0" + self.request_text.encode()
            ).digest(),
        )


@dataclass(frozen=True)
class SolutionPreparationFacts:
    solution_id: UUID
    command_id: UUID
    source_digest: bytes = field(repr=False)
    draft_digest: bytes = field(repr=False)
    assets_digest: bytes = field(repr=False)
    target_digest: bytes = field(repr=False)
    first_at: datetime = field(repr=False)
    execution_mode: str

    def __post_init__(self) -> None:
        if (
            any(not isinstance(v, UUID) or not v.int for v in (self.solution_id, self.command_id))
            or any(
                type(v) is not bytes or len(v) != 32
                for v in (
                    self.source_digest,
                    self.draft_digest,
                    self.assets_digest,
                    self.target_digest,
                )
            )
            or not isinstance(self.first_at, datetime)
            or self.first_at.utcoffset() is None
            or self.execution_mode not in ("serial_v1", "candidate_v2")
        ):
            raise ValueError(
                "Exact original source, draft, assets and first creation facts required"
            )

    def encode(self) -> bytes:
        raw = _MAGIC + _json(
            dict(
                solution_id=str(self.solution_id),
                command_id=str(self.command_id),
                source_digest=self.source_digest.hex(),
                draft_digest=self.draft_digest.hex(),
                assets_digest=self.assets_digest.hex(),
                target_digest=self.target_digest.hex(),
                first_at=self.first_at.isoformat(timespec="microseconds"),
                execution_mode=self.execution_mode,
            )
        )
        if len(raw) > 8192:
            raise ValueError("Frozen preparation facts exceed bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "SolutionPreparationFacts":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded preparation facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {
                "solution_id",
                "command_id",
                "source_digest",
                "draft_digest",
                "assets_digest",
                "target_digest",
                "first_at",
                "execution_mode",
            }:
                raise ValueError
            result = cls(
                UUID(value["solution_id"]),
                UUID(value["command_id"]),
                bytes.fromhex(value["source_digest"]),
                bytes.fromhex(value["draft_digest"]),
                bytes.fromhex(value["assets_digest"]),
                bytes.fromhex(value["target_digest"]),
                datetime.fromisoformat(value["first_at"]),
                value["execution_mode"],
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid original preparation facts") from None

    def validate_assets(
        self, incoming: SolutionPreparationInput, assets: PreparedAssetPlan
    ) -> None:
        if (
            self.solution_id != incoming.solution_id
            or self.command_id != incoming.command.command_id
            or assets.identities != incoming.identities
            or assets.digest != self.assets_digest
            or assets.first_at.isoformat() != self.first_at.isoformat()
            or assets.creation.execution_mode != self.execution_mode
            or assets.creation.organization_id != incoming.scope.organization_id
            or assets.creation.requested_by != "user:" + str(incoming.scope.operator_user_id)
            or assets.creation.run_id != incoming.identities.run_id
        ):
            raise ValueError("Original complete preparation assets differ")


def read_preparation_plan(
    incoming: SolutionPreparationInput,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, SolutionPreparationFacts, FrozenArchive]:
    facts, first = read_evaluation_state_plan(
        incoming.intent, winner, route_resolver=route_resolver
    )
    original = SolutionPreparationFacts.decode(facts.business_context)
    state = pb.MessagingBody.FromString(first.body).evaluation_state
    if (
        winner.operation_id != str(incoming.command.command_id)
        or original.solution_id != incoming.solution_id
        or original.command_id != incoming.command.command_id
        or facts.at != original.first_at
        or facts.previous_sequence != 0
        or facts.previous_version != 0
        or state.version != 1
        or state.status != "requested"
    ):
        raise ValueError("Exact original initial Solution preparation state required")
    return facts, original, first
