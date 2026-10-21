"""Frozen original preflight evidence; no model or automatic business retry."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
)

_MAGIC = b"qs-ai-preflight-operation/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate preflight fact")
        value[key] = item
    return value


def preflight_intent(run_id: UUID, version: int, organization_id: int) -> EvaluationStateIntent:
    if not isinstance(run_id, UUID) or type(organization_id) is not int:
        raise ValueError("Original worker preflight scope required")
    # This survives a committed checkpoint/progress change. Mutable source
    # snapshots belong to the winning plan, never this idempotency input.
    command = _json([str(run_id), version, organization_id, "preflight"])
    return EvaluationStateIntent(
        str(organization_id),
        str(run_id),
        f"preflight/{version}",
        version,
        hashlib.sha256(b"qs-ai-preflight-input/v1\0" + command).digest(),
    )


def source_snapshot_digest(checkpoint: Mapping[Any, Any], run: Mapping[Any, Any]) -> bytes:
    return hashlib.sha256(
        b"qs-ai-preflight-source/v1\0"
        + _json(
            {
                "run_id": run["run_id"],
                "organization_id": run["organization_id"],
                "definition_json": run["definition_json"],
                "progress_json": run["progress_json"],
                "checkpoint_run_id": checkpoint["run_id"],
                "version": checkpoint["version"],
                "checkpoint_json": checkpoint["checkpoint_json"],
            }
        )
    ).digest()


@dataclass(frozen=True)
class PreflightOperation:
    snapshot_digest: bytes = field(repr=False)
    evidence: PreflightEvidence = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.snapshot_digest) is not bytes or len(self.snapshot_digest) != 32:
            raise ValueError("Exact original preflight snapshot required")
        if not isinstance(self.evidence, PreflightEvidence):
            raise ValueError("Original preflight evidence required")

    def encode(self) -> bytes:
        evidence = asdict(self.evidence)
        evidence["evaluated_at"] = self.evidence.evaluated_at.isoformat(timespec="microseconds")
        raw = _MAGIC + _json({"snapshot_digest": self.snapshot_digest.hex(), "evidence": evidence})
        if len(raw) > 8192:
            raise ValueError("Original preflight evidence exceeds bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "PreflightOperation":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded preflight operation required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {"snapshot_digest", "evidence"}:
                raise ValueError
            fields = value["evidence"]
            if (
                not isinstance(fields, dict)
                or set(fields)
                != {
                    "case_id",
                    "status",
                    "evaluated_at",
                    "provider_call_count",
                    "rejection_reason",
                    "assertions",
                }
                or not isinstance(fields["assertions"], list)
            ):
                raise ValueError
            evidence = PreflightEvidence(
                **{
                    **fields,
                    "evaluated_at": datetime.fromisoformat(fields["evaluated_at"]),
                    "assertions": tuple(AssertionReceipt(**item) for item in fields["assertions"]),
                }
            )
            result = cls(bytes.fromhex(value["snapshot_digest"]), evidence)
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original preflight evidence") from None

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> "PreflightOperation":
        if facts.intent.operation_key != f"preflight/{facts.intent.expected_version}":
            raise ValueError("Original preflight operation identity required")
        result = cls.decode(facts.business_context)
        if result.evidence.evaluated_at != facts.at:
            raise ValueError("Original preflight evidence clock differs")
        return result
