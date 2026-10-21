"""Immutable non-command evaluation state plans, separate from business execution.

The host supplies an authorized stable operation and already sealed first wire.
These values authorize neither a model call nor an automatic Retry/Replan.
"""

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid5

from reliable_messaging.transaction import FrozenPlan, Request
from reliable_messaging.transport import physical_topic_v2

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    MessagingContractError,
    PreparedMessage,
    parse_body,
    valid_hash,
    valid_id,
    valid_number,
)
from qs_ai.infrastructure.workflow_transport.state_projection import next_evaluation_sequence
from qs_ai.infrastructure.workflow_transport.transaction_archive import (
    MAX_CONTEXT,
    FrozenArchive,
    decode_branch,
    freeze_branch,
)

_MAGIC = b"qs-ai-evaluation-state/v1\0"
_CONTEXT_MAGIC = b"qs-ai-evaluation-state/v2\0"
_MAX_VERSION = 2**63 - 1


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate state plan field")
        value[key] = item
    return value


@dataclass(frozen=True)
class EvaluationStateIntent:
    organization_id: str
    run_id: str
    operation_key: str
    expected_version: int
    source_digest: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.organization_id, str)
            or not valid_number(self.organization_id)
            or not isinstance(self.run_id, str)
            or not valid_id(self.run_id)
            or not isinstance(self.operation_key, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", self.operation_key) is None
            or type(self.expected_version) is not int
            or not 0 <= self.expected_version < _MAX_VERSION
            or type(self.source_digest) is not bytes
            or len(self.source_digest) != 32
        ):
            raise ValueError("Bounded original evaluation state intent required")

    def _fields(self) -> dict[str, Any]:
        return {
            "organization_id": self.organization_id,
            "run_id": self.run_id,
            "operation_key": self.operation_key,
            "expected_version": self.expected_version,
            "source_digest": self.source_digest.hex(),
        }

    @property
    def request(self) -> Request:
        return Request(
            "ai.mysql",
            "org:" + self.organization_id,
            f"ai-evaluation-state/{self.run_id}/{self.operation_key}",
            hashlib.sha256(
                b"qs-ai-evaluation-state-input/v1\0" + _canonical(self._fields())
            ).digest(),
        )


@dataclass(frozen=True)
class EvaluationStateFacts:
    intent: EvaluationStateIntent
    at: datetime
    previous_sequence: int
    previous_version: int
    business_context: bytes = field(default=b"", repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, EvaluationStateIntent)
            or not isinstance(self.at, datetime)
            or self.at.utcoffset() is None
            or any(
                type(value) is not int or not 0 <= value < _MAX_VERSION
                for value in (self.previous_sequence, self.previous_version)
            )
            or bool(self.previous_sequence) != bool(self.previous_version)
            or self.previous_version > self.intent.expected_version
            or type(self.business_context) is not bytes
            or len(self.business_context) > 8192
        ):
            raise ValueError("Exact original state clock and sequence evidence required")
        object.__setattr__(self, "at", self.at.astimezone(UTC))

    @property
    def sequence(self) -> int:
        return next_evaluation_sequence(
            self.previous_sequence, self.previous_version, self.intent.expected_version + 1
        )

    def encode(self) -> bytes:
        value = {
            **self.intent._fields(),
            "at": self.at.isoformat(timespec="microseconds"),
            "previous_sequence": self.previous_sequence,
            "previous_version": self.previous_version,
        }
        if self.business_context:
            value["business_context"] = base64.b64encode(self.business_context).decode("ascii")
        # Every already-frozen v1 byte remains unchanged when no extension exists.
        raw = (_CONTEXT_MAGIC if self.business_context else _MAGIC) + _canonical(value)
        if len(raw) > MAX_CONTEXT:
            raise ValueError("State plan context exceeds bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "EvaluationStateFacts":
        if type(raw) is not bytes or len(raw) > MAX_CONTEXT:
            raise ValueError("Explicit bounded state plan context required")
        extended = raw.startswith(_CONTEXT_MAGIC)
        magic = _CONTEXT_MAGIC if extended else _MAGIC
        if not raw.startswith(magic):
            raise ValueError("Explicit bounded state plan context required")
        try:
            value = json.loads(raw[len(magic) :], object_pairs_hook=_unique)
            fields = {
                "organization_id",
                "run_id",
                "operation_key",
                "expected_version",
                "source_digest",
                "at",
                "previous_sequence",
                "previous_version",
            }
            if extended:
                fields.add("business_context")
            if not isinstance(value, dict) or set(value) != fields:
                raise ValueError
            result = cls(
                EvaluationStateIntent(
                    value["organization_id"],
                    value["run_id"],
                    value["operation_key"],
                    value["expected_version"],
                    bytes.fromhex(value["source_digest"]),
                ),
                datetime.fromisoformat(value["at"]),
                value["previous_sequence"],
                value["previous_version"],
                base64.b64decode(value["business_context"], validate=True) if extended else b"",
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen evaluation state context") from None


def freeze_evaluation_state_plan(
    facts: EvaluationStateFacts, operation_id: str, prepared: PreparedMessage
) -> FrozenPlan:
    """Freeze the first already sealed message, without SQL, clock, keys or PUB."""
    if not isinstance(operation_id, str) or not valid_id(operation_id):
        raise ValueError("Canonical frozen state operation required")
    branch = freeze_branch(
        prepared,
        facts.intent.organization_id,
        facts.sequence,
        False,
        True,
        prepared.envelope.message_id,
        physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1"),
        "m3-1",
        context=facts.encode(),
    )
    request = facts.intent.request
    plan = FrozenPlan(
        request.owner, request.scope, operation_id, request.key, request.input_hash, (branch,)
    )
    read_evaluation_state_plan(facts.intent, plan)
    return plan


def read_evaluation_state_plan(
    intent: EvaluationStateIntent, plan: FrozenPlan
) -> tuple[EvaluationStateFacts, FrozenArchive]:
    intent.request.match(plan)
    if len(plan.branches) != 1:
        raise MessagingContractError("One original evaluation state branch required")
    branch = plan.branches[0]
    archive = decode_branch(branch)
    facts = EvaluationStateFacts.decode(archive.context)
    header = archive.envelope()
    if (
        facts.intent != intent
        or branch.physical_topic != physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
        or branch.branch_id != header.message_id
        or header.kind != pb.EVALUATION_STATE
        or header.producer != "qs-ai"
        or header.destination != "qs-server"
        or header.aggregate_key != intent.run_id
        or header.correlation_command_id
        or header.original_occurred_at != facts.at.isoformat(timespec="microseconds")
        or str(archive.organization_id) != intent.organization_id
        or archive.sequence != facts.sequence
        or archive.ordered
        or not archive.requires_receipt
    ):
        raise MessagingContractError("Frozen original evaluation state binding differs")
    state = parse_body(header, archive.body).evaluation_state
    if (
        state.run_id != intent.run_id
        or state.organization_id != intent.organization_id
        or state.version != intent.expected_version + 1
        or state.event_sequence != facts.sequence
        or header.message_id != str(uuid5(UUID(state.run_id), f"state:{state.version}"))
        or not 0 < len(state.status.encode()) <= 128
        or not state.release_fingerprint.startswith("sha256:")
        or not valid_hash(state.release_fingerprint.removeprefix("sha256:"))
    ):
        raise MessagingContractError("Frozen evaluation state projection differs")
    return facts, archive
