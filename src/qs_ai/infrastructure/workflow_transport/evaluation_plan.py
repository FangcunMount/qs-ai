"""Bounded host-only evaluation plan facts; SDK codec and business wire stay unchanged."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid5

from reliable_messaging.transaction import FrozenPlan

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.workflow_transport.messaging import FIELDS, valid_id
from qs_ai.infrastructure.workflow_transport.transaction_archive import MAX_CONTEXT
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)

_MAGIC = b"qs-ai-evaluation-admission/v1\0"


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate evaluation fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class EvaluationPlanFacts:
    input_hash: bytes = field(repr=False)
    kind: int
    at: datetime
    run_id: str
    reservation_token: str
    source_digest: bytes | None = field(default=None, repr=False)
    previous_sequence: int = 0
    previous_version: int = 0

    def __post_init__(self) -> None:
        if type(self.kind) is not int or self.kind not in (
            pb.EVALUATION_START,
            pb.EVALUATION_CANCEL,
        ):
            raise ValueError("Explicit evaluation kind required")
        if type(self.input_hash) is not bytes or len(self.input_hash) != 32:
            raise ValueError("Original evaluation input digest required")
        if self.source_digest is not None and (
            type(self.source_digest) is not bytes or len(self.source_digest) != 32
        ):
            raise ValueError("Exact original evaluation source digest required")
        if not isinstance(self.at, datetime) or self.at.utcoffset() is None:
            raise ValueError("Aware frozen evaluation clock required")
        object.__setattr__(self, "at", self.at.astimezone(UTC))
        if not isinstance(self.run_id, str) or len(self.run_id.encode()) > 128:
            raise ValueError("Bounded original evaluation target required")
        if not isinstance(self.reservation_token, str) or not valid_id(self.reservation_token):
            raise ValueError("Canonical frozen Inbox identity required")
        if any(
            type(value) is not int or not 0 <= value < 2**63 - 1
            for value in (self.previous_sequence, self.previous_version)
        ) or bool(self.previous_sequence) != bool(self.previous_version):
            raise ValueError("Bounded original evaluation sequence required")

    def validate_clock(self, now: datetime) -> None:
        if (
            not isinstance(now, datetime)
            or now.utcoffset() is None
            or now.astimezone(UTC).date() != self.at.date()
        ):
            raise ConfigurationPlanConflict("Frozen evaluation budget day changed")

    def encode(self) -> bytes:
        value = {
            "input_hash": self.input_hash.hex(),
            "kind": self.kind,
            "at": self.at.isoformat(timespec="microseconds"),
            "run_id": self.run_id,
            "reservation_token": self.reservation_token,
            "source_digest": None if self.source_digest is None else self.source_digest.hex(),
            "previous_sequence": self.previous_sequence,
            "previous_version": self.previous_version,
        }
        raw = (
            _MAGIC
            + json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        )
        if len(raw) > MAX_CONTEXT:
            raise ValueError("Frozen evaluation facts exceed bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "EvaluationPlanFacts":
        if type(raw) is not bytes or len(raw) > MAX_CONTEXT or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded evaluation facts version required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_object)
            if not isinstance(value, dict) or set(value) != {
                "input_hash",
                "kind",
                "at",
                "run_id",
                "reservation_token",
                "source_digest",
                "previous_sequence",
                "previous_version",
            }:
                raise ValueError
            result = cls(
                bytes.fromhex(value["input_hash"]),
                value["kind"],
                datetime.fromisoformat(value["at"]),
                value["run_id"],
                value["reservation_token"],
                None if value["source_digest"] is None else bytes.fromhex(value["source_digest"]),
                value["previous_sequence"],
                value["previous_version"],
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen evaluation facts") from None

    @classmethod
    def from_plan(cls, incoming: FrozenCommandInput, plan: FrozenPlan) -> "EvaluationPlanFacts":
        archives = read_admission_plan(incoming, plan)
        result = cls.decode(archives[0].context)
        kind = incoming.envelope().kind
        if kind not in (pb.EVALUATION_START, pb.EVALUATION_CANCEL):
            raise ValueError("Original evaluation command required")
        request = getattr(incoming.body_message(), FIELDS[kind])
        if (
            result.kind != kind
            or result.input_hash != incoming.request.input_hash
            or result.run_id != request.scope.run_id
        ):
            raise ValueError("Frozen evaluation input differs")
        states = [a for a in archives if a.envelope().kind == pb.EVALUATION_STATE]
        if len(states) > 1:
            raise ValueError("One original evaluation state required")
        for archive in archives:
            envelope = archive.envelope()
            if datetime.fromisoformat(envelope.original_occurred_at) != result.at:
                raise ValueError("Frozen evaluation first clock differs")
            body = pb.MessagingBody.FromString(archive.body)
            if envelope.kind == pb.EVALUATION_STATE:
                state = body.evaluation_state
                if (
                    result.source_digest is None
                    or not valid_id(result.run_id)
                    or (
                        state.run_id != result.run_id
                        or state.organization_id != incoming.organization_id
                        or state.version != request.expected_version + 1
                        or result.previous_version > request.expected_version
                        or state.event_sequence != result.previous_sequence + 1
                        or envelope.message_id
                        != str(uuid5(UUID(result.run_id), f"state:{state.version}"))
                    )
                ):
                    raise ValueError("Frozen evaluation state identity or sequence differs")
            elif (
                body.command_receipt.HasField("evaluation_receipt")
                and body.command_receipt.evaluation_receipt.run_id != result.run_id
            ):
                raise ValueError("Frozen evaluation receipt target differs")
        return result
