"""Bounded local START facts; no clocks, identities, transactions or I/O are created.

The durable reservation winner owns these facts. A losing candidate must read
this context before constructing its original-root business action.
"""

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from reliable_messaging.transaction import FrozenPlan

from qs_ai.application.interpretation.service import IdentityRole, fingerprint
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.interpretation.model import EvidenceItem, Fact
from qs_ai.infrastructure.persistence.mysql.execution_configurations import PlannedConfiguration
from qs_ai.infrastructure.workflow_transport.messaging import valid_id
from qs_ai.infrastructure.workflow_transport.transaction_archive import MAX_CONTEXT
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)

_MAGIC = b"qs-ai-start-plan/v1\0"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITIES = (
    "session_id",
    "run_id",
    "evidence_id",
    "job_id",
    "reservation_token",
    "state_event_id",
)
_CONFIG_FIELDS = {
    "session_id",
    "evidence_set_id",
    "evidence_fingerprint",
    "publication_id",
    "publication_sha256",
    "pointer_version",
    "selector_query",
}


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate frozen START context field")
        result[key] = value
    return result


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


@dataclass(frozen=True)
class StartPlanFacts:
    input_hash: bytes = field(repr=False)
    at: datetime
    session_id: str
    run_id: str
    evidence_id: str
    job_id: str
    reservation_token: str
    state_event_id: str
    configuration: PlannedConfiguration | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.input_hash, (bytes, bytearray, memoryview))
            or memoryview(self.input_hash).nbytes != 32
        ):
            raise ValueError("Original START input hash required")
        object.__setattr__(self, "input_hash", bytes(self.input_hash))
        if not isinstance(self.at, datetime) or self.at.utcoffset() is None:
            raise ValueError("Frozen aware START clock required")
        object.__setattr__(self, "at", self.at.astimezone(UTC))
        if any(
            not isinstance(v := getattr(self, name), str) or not valid_id(v) for name in _IDENTITIES
        ):
            raise ValueError("Canonical frozen START identities required")
        config = self.configuration
        if config is not None:
            if (
                not isinstance(config, PlannedConfiguration)
                or config.session_id != self.session_id
                or config.evidence_set_id != self.evidence_id
                or not isinstance(config.publication_id, str)
                or not valid_id(config.publication_id)
                or not isinstance(config.evidence_fingerprint, str)
                or not _HASH.fullmatch(config.evidence_fingerprint)
                or not isinstance(config.publication_sha256, str)
                or not _HASH.fullmatch(config.publication_sha256)
                or type(config.pointer_version) is not int
                or not 0 < config.pointer_version < 2**64
                or not isinstance(config.selector_query, str)
                or not 0 < len(config.selector_query.encode()) <= 4096
            ):
                raise ValueError("Frozen START configuration differs from local identities")
            try:
                query = json.loads(config.selector_query, object_pairs_hook=_object)
                if not isinstance(query, dict):
                    raise ValueError
                canonical = json.dumps(
                    query,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                if canonical != config.selector_query:
                    raise ValueError
            except (ValueError, TypeError, RecursionError):
                raise ValueError("Canonical frozen configuration selector required") from None

    def identity(self, role: IdentityRole) -> str:
        return {"session": self.session_id, "run": self.run_id, "evidence": self.evidence_id}[role]

    def encode(self) -> bytes:
        value = {
            "input_hash": self.input_hash.hex(),
            "at": self.at.isoformat(timespec="microseconds"),
            "ids": {name: getattr(self, name) for name in _IDENTITIES},
            "configuration": asdict(self.configuration) if self.configuration is not None else None,
        }
        raw = _MAGIC + _canonical(value)
        if len(raw) > MAX_CONTEXT:
            raise ValueError("Frozen START context exceeds bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "StartPlanFacts":
        if (
            not isinstance(raw, bytes)
            or not len(_MAGIC) < len(raw) <= MAX_CONTEXT
            or not raw.startswith(_MAGIC)
        ):
            raise ValueError("Explicit bounded START context version required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_object)
            if not isinstance(value, dict) or set(value) != {
                "input_hash",
                "at",
                "ids",
                "configuration",
            }:
                raise ValueError("Frozen START context fields differ")
            ids, config = value["ids"], value["configuration"]
            if not isinstance(ids, dict) or set(ids) != set(_IDENTITIES):
                raise ValueError("Frozen START identity fields differ")
            if config is not None and (
                not isinstance(config, dict) or set(config) != _CONFIG_FIELDS
            ):
                raise ValueError("Frozen START configuration fields differ")
            result = cls(
                bytes.fromhex(value["input_hash"]),
                datetime.fromisoformat(value["at"]),
                **ids,
                configuration=PlannedConfiguration(**config) if config is not None else None,
            )
            if result.encode() != raw:
                raise ValueError("Noncanonical frozen START context")
            return result
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, AttributeError):
            raise ValueError("Invalid frozen START context") from None

    @classmethod
    def from_plan(cls, incoming: FrozenCommandInput, plan: FrozenPlan) -> "StartPlanFacts":
        archives = read_admission_plan(incoming, plan)
        if incoming.envelope().kind != pb.START:
            raise ValueError("Original START command required")
        result = cls.decode(archives[0].context)
        if result.input_hash != incoming.request.input_hash:
            raise ValueError("Frozen START input differs")
        if result.configuration is not None:
            evidence = tuple(
                EvidenceItem(
                    item.assessment_id,
                    item.testee_id,
                    item.report_id,
                    item.source_version,
                    tuple(Fact(fact.ref, fact.value) for fact in item.facts),
                )
                for item in incoming.body_message().start.evidence
            )
            if result.configuration.evidence_fingerprint != fingerprint(
                [asdict(item) for item in evidence]
            ):
                raise ValueError("Frozen START configuration binds another evidence input")
        for value in archives:
            envelope = value.envelope()
            body = pb.MessagingBody.FromString(value.body)
            if envelope.kind == pb.COMMAND_RECEIPT and body.command_receipt.HasField(
                "workflow_receipt"
            ):
                receipt = body.command_receipt.workflow_receipt
                if receipt.session_id != result.session_id or receipt.run_id != result.run_id:
                    raise ValueError("Frozen START receipt identities differ")
            elif (
                envelope.kind == pb.INTERPRETATION_STATE
                and envelope.message_id != result.state_event_id
            ):
                raise ValueError("Frozen START state identity differs")
        return result
