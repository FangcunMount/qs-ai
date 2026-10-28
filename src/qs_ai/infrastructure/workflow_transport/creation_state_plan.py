"""Stable authorized creation input; first clock/mode belong to the frozen winner."""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import RunCreationPlan
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    FrozenPolicyDocument,
    execution_policy,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

_MAGIC = b"qs-ai-creation-state/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate frozen creation fact")
        value[key] = item
    return value


@dataclass(frozen=True)
class CreationStateInput:
    scope: ManagementScope
    release: EvidenceReleaseIdentity
    reason: str = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ManagementScope)
            or not isinstance(self.release, EvidenceReleaseIdentity)
            or type(self.reason) is not str
            or not self.reason.strip()
            or len(self.reason.strip().encode()) > 1000
            or any(value in self.reason for value in "<>")
        ):
            raise ValueError("Original scoped, explicit creation input required")
        object.__setattr__(self, "reason", self.reason.strip())

    @property
    def intent(self) -> EvaluationStateIntent:
        raw = json.dumps(
            dict(
                run_id=str(self.scope.run_id),
                organization_id=self.scope.organization_id,
                operator_user_id=self.scope.operator_user_id,
                release=asdict(self.release),
                reason=self.reason,
            ),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
        return EvaluationStateIntent(
            str(self.scope.organization_id),
            str(self.scope.run_id),
            "governance/create/" + str(self.scope.run_id),
            0,
            hashlib.sha256(b"qs-ai-creation-input/v1\0" + raw).digest(),
        )


@dataclass(frozen=True)
class CreationPlanFacts:
    definition_digest: bytes = field(repr=False)
    execution_mode: str
    created_at: datetime = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.definition_digest) is not bytes
            or len(self.definition_digest) != 32
            or self.execution_mode not in ("serial_v1", "candidate_v2")
            or not isinstance(self.created_at, datetime)
            or self.created_at.utcoffset() is None
        ):
            raise ValueError("Exact first definition, mode and creation clock required")

    def encode(self) -> bytes:
        return _MAGIC + _json(
            dict(
                definition_digest=self.definition_digest.hex(),
                execution_mode=self.execution_mode,
                created_at=self.created_at.isoformat(timespec="microseconds"),
            )
        )

    @classmethod
    def decode(cls, raw: bytes) -> "CreationPlanFacts":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded first creation facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {
                "definition_digest",
                "execution_mode",
                "created_at",
            }:
                raise ValueError
            result = cls(
                bytes.fromhex(value["definition_digest"]),
                value["execution_mode"],
                datetime.fromisoformat(value["created_at"]),
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen creation facts") from None

    @classmethod
    def from_creation(
        cls, incoming: CreationStateInput, plan: RunCreationPlan
    ) -> "CreationPlanFacts":
        value = json.loads(plan.definition_json)
        result = cls(
            hashlib.sha256(plan.definition_json.encode()).digest(),
            plan.execution_mode,
            datetime.fromisoformat(value["audit"]["created_at"]),
        )
        result.validate(incoming, plan)
        return result

    def validate(self, incoming: CreationStateInput, plan: RunCreationPlan) -> None:
        value = json.loads(plan.definition_json)
        if (
            plan.run_id != incoming.scope.run_id
            or plan.organization_id != incoming.scope.organization_id
            or plan.requested_by != incoming.scope.actor
            or plan.execution_mode != self.execution_mode
            or hashlib.sha256(plan.definition_json.encode()).digest() != self.definition_digest
            or value["run_id"] != str(incoming.scope.run_id)
            or value["schema_version"] != "qs-ai-evaluation-run-creation/v1"
            or value["status"] != "requested"
            or value["release"] != asdict(incoming.release)
            or value["release_fingerprint"] != incoming.release.fingerprint()
            or value["audit"]
            != dict(
                organization_id=incoming.scope.organization_id,
                requested_by=incoming.scope.actor,
                request_reason=incoming.reason,
                created_at=datetime.fromisoformat(value["audit"]["created_at"]).isoformat(),
            )
            or datetime.fromisoformat(value["audit"]["created_at"]) != self.created_at
            or plan.policy
            != execution_policy(
                FrozenPolicyDocument(
                    incoming.release.execution_policy, value["execution_policy_json"]
                )
            )
        ):
            raise ValueError("Original frozen creation input differs")


def read_creation_state_plan(
    incoming: CreationStateInput,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, CreationPlanFacts, FrozenArchive]:
    facts, archived = read_evaluation_state_plan(
        incoming.intent, winner, route_resolver=route_resolver
    )
    original = CreationPlanFacts.decode(facts.business_context)
    state = pb.MessagingBody.FromString(archived.body).evaluation_state
    if (
        winner.operation_id != str(incoming.scope.run_id)
        or state.version != 1
        or state.status != "requested"
        or facts.previous_sequence != 0
        or facts.previous_version != 0
        or state.release_fingerprint != incoming.release.fingerprint()
        or facts.at != original.created_at
    ):
        raise ValueError("Exact original initial creation state required")
    return facts, original, archived
