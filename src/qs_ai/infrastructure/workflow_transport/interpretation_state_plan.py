"""Frozen non-command interpretation state; original business authority stays with its root."""

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from reliable_messaging.transaction import FrozenPlan, Request
from reliable_messaging.transport import physical_topic_v2

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.interpretation.model import Status
from qs_ai.infrastructure.workflow_transport.frozen_start_events import (
    InterpretationStateTranscription,
)
from qs_ai.infrastructure.workflow_transport.messaging import (
    EVENTS,
    MessagingContractError,
    PreparedMessage,
    parse_body,
    valid_id,
    valid_number,
)
from qs_ai.infrastructure.workflow_transport.transaction_archive import (
    MAX_CONTEXT,
    FrozenArchive,
    decode_branch,
    freeze_branch,
)

_MAGIC = b"qs-ai-interpretation-state/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate original interpretation state fact")
        value[key] = item
    return value


@dataclass(frozen=True)
class InterpretationStateIntent:
    organization_id: str
    session_id: str
    request_id: str
    operation_key: str
    expected_version: int
    input_hash: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.organization_id, str)
            or not valid_number(self.organization_id)
            or any(
                not isinstance(v, str) or not valid_id(v)
                for v in (self.session_id, self.request_id)
            )
            or not isinstance(self.operation_key, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", self.operation_key) is None
            or type(self.expected_version) is not int
            or not 0 < self.expected_version < 2**63 - 1
            or type(self.input_hash) is not bytes
            or len(self.input_hash) != 32
        ):
            raise ValueError("Exact original scoped interpretation state intent required")

    def fields(self) -> dict[str, Any]:
        return dict(
            organization_id=self.organization_id,
            session_id=self.session_id,
            request_id=self.request_id,
            operation_key=self.operation_key,
            expected_version=self.expected_version,
            input_hash=self.input_hash.hex(),
        )

    @property
    def request(self) -> Request:
        return Request(
            "ai.mysql",
            "org:" + self.organization_id,
            f"ai-interpretation-state/{self.session_id}/{self.operation_key}",
            hashlib.sha256(
                b"qs-ai-interpretation-state-input/v1\0" + _json(self.fields())
            ).digest(),
        )


@dataclass(frozen=True)
class InterpretationStateFacts:
    intent: InterpretationStateIntent
    at: datetime
    business_context: bytes = field(default=b"", repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, InterpretationStateIntent)
            or not isinstance(self.at, datetime)
            or self.at.utcoffset() is None
            or type(self.business_context) is not bytes
            or len(self.business_context) > 8192
        ):
            raise ValueError("Exact original interpretation state clock/context required")
        object.__setattr__(self, "at", self.at.astimezone(UTC))

    def encode(self) -> bytes:
        raw = _MAGIC + _json(
            {
                **self.intent.fields(),
                "at": self.at.isoformat(timespec="microseconds"),
                "business_context": base64.b64encode(self.business_context).decode("ascii"),
            }
        )
        if len(raw) > MAX_CONTEXT:
            raise ValueError("Original interpretation facts exceed bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "InterpretationStateFacts":
        if type(raw) is not bytes or len(raw) > MAX_CONTEXT or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded original interpretation state facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {
                "organization_id",
                "session_id",
                "request_id",
                "operation_key",
                "expected_version",
                "input_hash",
                "at",
                "business_context",
            }:
                raise ValueError
            result = cls(
                InterpretationStateIntent(
                    value["organization_id"],
                    value["session_id"],
                    value["request_id"],
                    value["operation_key"],
                    value["expected_version"],
                    bytes.fromhex(value["input_hash"]),
                ),
                datetime.fromisoformat(value["at"]),
                base64.b64decode(value["business_context"], validate=True),
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen interpretation state facts") from None


def freeze_interpretation_state_plan(
    facts: InterpretationStateFacts, operation_id: str, prepared: PreparedMessage
) -> FrozenPlan:
    if not isinstance(operation_id, str) or not valid_id(operation_id):
        raise ValueError("Canonical original interpretation operation required")
    branch = freeze_branch(
        prepared,
        facts.intent.organization_id,
        facts.intent.expected_version + 1,
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
    read_interpretation_state_plan(facts.intent, plan)
    return plan


def read_interpretation_state_plan(
    intent: InterpretationStateIntent, plan: FrozenPlan
) -> tuple[InterpretationStateFacts, FrozenArchive]:
    intent.request.match(plan)
    if len(plan.branches) != 1:
        raise MessagingContractError("One original interpretation state branch required")
    branch = plan.branches[0]
    archive = decode_branch(branch)
    facts = InterpretationStateFacts.decode(archive.context)
    header = archive.envelope()
    if (
        facts.intent != intent
        or branch.physical_topic != physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
        or branch.branch_id != header.message_id
        or header.kind != pb.INTERPRETATION_STATE
        or header.producer != "qs-ai"
        or header.destination != "qs-server"
        or header.aggregate_key != intent.request_id
        or header.correlation_command_id
        or header.original_occurred_at != facts.at.isoformat(timespec="microseconds")
        or str(archive.organization_id) != intent.organization_id
        or archive.sequence != intent.expected_version + 1
        or archive.ordered
        or not archive.requires_receipt
    ):
        raise MessagingContractError("Frozen original interpretation state binding differs")
    state = parse_body(header, archive.body).interpretation_state
    if (
        state.event_id != header.message_id
        or not valid_id(state.event_id)
        or state.session_id != intent.session_id
        or state.request_id != intent.request_id
        or state.actor.org_id != intent.organization_id
        or not state.actor.subject_id
        or not valid_number(state.testee_id)
        or state.version != intent.expected_version + 1
        or state.status not in {s.value for s in Status}
    ):
        raise MessagingContractError("Frozen original interpretation projection differs")
    return facts, archive


class FrozenInterpretationStateEvents(InterpretationStateTranscription):
    """A standalone original state has no synthetic command or Inbox receipt."""

    def __init__(self, intent: InterpretationStateIntent, plan: FrozenPlan) -> None:
        _, archive = read_interpretation_state_plan(intent, plan)
        expected = pb.MessagingBody.FromString(archive.body).interpretation_state
        super().__init__(plan.digest, expected)
