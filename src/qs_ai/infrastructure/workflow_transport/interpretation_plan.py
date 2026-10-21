"""Immutable interpretation mutation resources and target-bound pre-transaction authorization."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from reliable_messaging.transaction import FrozenPlan

from qs_ai.application.interpretation.service import IdentityRole
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.workflow_transport.frozen_authorization import EvidenceAuthorization
from qs_ai.infrastructure.workflow_transport.messaging import FIELDS, valid_id
from qs_ai.infrastructure.workflow_transport.transaction_archive import MAX_CONTEXT
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)

_MAGIC_BY_KIND = {
    pb.CHANGE: b"qs-ai-change-plan/v1\0",
    pb.PARTICIPANT_RETRY: b"qs-ai-participant-retry-plan/v1\0",
}
_IDS = ("run_id", "job_id", "reservation_token", "state_event_id")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate frozen CHANGE field")
        result[key] = value
    return result


@dataclass(frozen=True)
class InterpretationMutationFacts:
    input_hash: bytes = field(repr=False)
    at: datetime
    session_id: str
    run_id: str
    job_id: str
    reservation_token: str
    state_event_id: str
    authorization: EvidenceAuthorization | None = field(default=None, repr=False)
    kind: int = pb.CHANGE
    audit_digest: bytes | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if type(self.kind) is not int or self.kind not in _MAGIC_BY_KIND:
            raise ValueError("Explicit interpretation mutation kind required")
        if self.audit_digest is not None and (
            type(self.audit_digest) is not bytes
            or len(self.audit_digest) != 32
            or self.kind != pb.PARTICIPANT_RETRY
        ):
            raise ValueError("Explicit retry audit digest required")

        if type(self.input_hash) is not bytes or len(self.input_hash) != 32:
            raise ValueError("Original CHANGE input digest required")
        if not isinstance(self.at, datetime) or self.at.utcoffset() is None:
            raise ValueError("Aware frozen CHANGE clock required")
        object.__setattr__(self, "at", self.at.astimezone(UTC))
        # Preserve malformed request targets for the original durable refusal.
        # Only valid targets can pass original command validation in the root.
        if not isinstance(self.session_id, str) or len(self.session_id.encode()) > 128:
            raise ValueError("Bounded original CHANGE target required")
        if any(not isinstance(v := getattr(self, name), str) or not valid_id(v) for name in _IDS):
            raise ValueError("Canonical frozen CHANGE identities required")
        if self.authorization is not None and not isinstance(
            self.authorization, EvidenceAuthorization
        ):
            raise ValueError("Typed original authorization observation required")

    def identity(self, role: IdentityRole) -> str:
        if role != "run":
            raise RuntimeError("CHANGE cannot create START identities")
        return self.run_id

    def validate_observation(self, now: datetime) -> None:
        if not isinstance(now, datetime) or now.utcoffset() is None:
            raise ConfigurationPlanConflict("Authoritative root clock unavailable")
        if now.astimezone(UTC).date() != self.at.date():
            raise ConfigurationPlanConflict("Frozen CHANGE budget day changed")
        if self.authorization is not None:
            age = (now - self.authorization.observed_at).total_seconds()
            if not 0 <= age <= 60:
                raise ConfigurationPlanConflict("Frozen CHANGE authorization observation expired")

    def encode(self) -> bytes:
        proof = self.authorization
        value = {
            "input_hash": self.input_hash.hex(),
            "at": self.at.isoformat(timespec="microseconds"),
            "session_id": self.session_id,
            "ids": {name: getattr(self, name) for name in _IDS},
            "authorization": None
            if proof is None
            else {
                "target_hash": proof.target_hash.hex(),
                "allowed": proof.allowed,
                "observed_at": proof.observed_at.isoformat(timespec="microseconds"),
            },
        }
        if self.kind == pb.PARTICIPANT_RETRY:
            value["audit_digest"] = None if self.audit_digest is None else self.audit_digest.hex()
        raw = (
            _MAGIC_BY_KIND[self.kind]
            + json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        )
        if len(raw) > MAX_CONTEXT:
            raise ValueError("Frozen CHANGE context exceeds bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "InterpretationMutationFacts":
        if type(raw) is not bytes or not len(raw) <= MAX_CONTEXT:
            raise ValueError("Explicit bounded interpretation context required")
        kind = next((k for k, magic in _MAGIC_BY_KIND.items() if raw.startswith(magic)), None)
        if kind is None:
            raise ValueError("Explicit interpretation context version required")
        magic = _MAGIC_BY_KIND[kind]
        try:
            value = json.loads(raw[len(magic) :], object_pairs_hook=_object)
            fields = {"input_hash", "at", "session_id", "ids", "authorization"}
            if kind == pb.PARTICIPANT_RETRY:
                fields.add("audit_digest")
            if not isinstance(value, dict) or set(value) != fields:
                raise ValueError
            ids, proof = value["ids"], value["authorization"]
            if not isinstance(ids, dict) or set(ids) != set(_IDS):
                raise ValueError
            if proof is not None and (
                not isinstance(proof, dict)
                or set(proof) != {"target_hash", "allowed", "observed_at"}
            ):
                raise ValueError
            result = cls(
                bytes.fromhex(value["input_hash"]),
                datetime.fromisoformat(value["at"]),
                value["session_id"],
                **ids,
                kind=kind,
                audit_digest=(
                    None
                    if kind == pb.CHANGE or value["audit_digest"] is None
                    else bytes.fromhex(value["audit_digest"])
                ),
                authorization=None
                if proof is None
                else EvidenceAuthorization(
                    bytes.fromhex(proof["target_hash"]),
                    proof["allowed"],
                    datetime.fromisoformat(proof["observed_at"]),
                ),
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen CHANGE context") from None

    @classmethod
    def from_plan(
        cls, incoming: FrozenCommandInput, plan: FrozenPlan
    ) -> "InterpretationMutationFacts":
        archives = read_admission_plan(incoming, plan)
        kind = incoming.envelope().kind
        if kind not in _MAGIC_BY_KIND:
            raise ValueError("Original CHANGE or PARTICIPANT_RETRY command required")
        result = cls.decode(archives[0].context)
        request = getattr(incoming.body_message(), FIELDS[kind])
        if (
            result.kind != kind
            or result.input_hash != incoming.request.input_hash
            or result.session_id != request.session_id
        ):
            raise ValueError("Frozen CHANGE input differs")
        has_state = any(value.envelope().kind == pb.INTERPRETATION_STATE for value in archives)
        if kind == pb.PARTICIPANT_RETRY and has_state and result.audit_digest is None:
            raise ValueError("Frozen retry state requires exact audit proof")
        for value in archives:
            envelope = value.envelope()
            if datetime.fromisoformat(envelope.original_occurred_at) != result.at:
                raise ValueError("Frozen CHANGE first clock differs")
            body = pb.MessagingBody.FromString(value.body)
            if (
                envelope.kind == pb.INTERPRETATION_STATE
                and envelope.message_id != result.state_event_id
            ):
                raise ValueError("Frozen CHANGE state identity differs")
            if envelope.kind == pb.COMMAND_RECEIPT and body.command_receipt.HasField(
                "workflow_receipt"
            ):
                receipt = body.command_receipt.workflow_receipt
                if receipt.session_id != result.session_id or (
                    has_state
                    and (kind == pb.PARTICIPANT_RETRY or request.action == "answer")
                    and receipt.run_id != result.run_id
                ):
                    raise ValueError("Frozen CHANGE receipt identities differ")
        return result
