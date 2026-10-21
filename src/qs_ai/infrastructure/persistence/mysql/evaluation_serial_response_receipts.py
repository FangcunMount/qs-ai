"""Original serial dispatch response evidence; no fabricated candidate claim or business retry."""

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import BindingError
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import (
    decode as decode_checkpoint,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import (
    encode as encode_checkpoint,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    decode as decode_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import PreparedStep
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_response_receipts,
    evaluation_runs,
)

_VERSION = "qs-ai-evaluation-serial-response/v1"
_MAX_BYTES = 4 * 1024 * 1024


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate original serial response field")
        value[key] = item
    return value


@dataclass(frozen=True)
class SerialResponseReference:
    run_id: UUID
    organization_id: int
    version: int
    checkpoint: ExecutionCheckpoint

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, UUID)
            or type(self.organization_id) is not int
            or not 0 < self.organization_id < 2**63
            or type(self.version) is not int
            or not 0 < self.version < 2**63
            or not isinstance(self.checkpoint, ExecutionCheckpoint)
            or self.checkpoint.phase != "dispatching"
        ):
            raise ValueError("Exact original serial dispatch reference required")

    @classmethod
    def from_prepared(
        cls, organization_id: int, prepared: PreparedStep
    ) -> "SerialResponseReference":
        if prepared.claim is not None or prepared.state.checkpoint is None:
            raise ValueError("Original serial dispatch required; candidate ownership is distinct")
        if prepared.state.checkpoint != prepared.cp.mark_dispatching(
            prepared.cp.owner, prepared.at
        ):
            raise ValueError("Original serial prepared/dispatch identity differs")
        return cls(
            prepared.state.run_id,
            organization_id,
            prepared.state.version,
            prepared.state.checkpoint,
        )

    def fields(self) -> dict[str, Any]:
        return dict(
            run_id=str(self.run_id),
            organization_id=self.organization_id,
            version=self.version,
            checkpoint=encode_checkpoint(self.checkpoint),
        )


def encode(reference: SerialResponseReference, result: EvaluationResponse) -> str:
    if (
        not isinstance(reference, SerialResponseReference)
        or not isinstance(result, EvaluationResponse)
        or reference.checkpoint.dispatch_started_at is None
        or result.finished_at < reference.checkpoint.dispatch_started_at
        or (
            result.response is not None
            and result.response.invocation_id != reference.checkpoint.invocation_id
        )
    ):
        raise ValueError("Original serial response differs from dispatch")
    raw = _json(dict(schema_version=_VERSION, reference=reference.fields(), result=result.encode()))
    if len(raw.encode()) > _MAX_BYTES:
        raise ValueError("Original serial response exceeds the explicit evidence bound")
    return raw


def decode(reference: SerialResponseReference, raw: str, digest: str) -> EvaluationResponse:
    if (
        not isinstance(raw, str)
        or len(raw.encode()) > _MAX_BYTES
        or hashlib.sha256(raw.encode()).hexdigest() != digest
    ):
        raise ValueError("Original serial response digest or bound differs")
    try:
        value = json.loads(raw, object_pairs_hook=_unique)
        if (
            set(value) != {"schema_version", "reference", "result"}
            or value["schema_version"] != _VERSION
            or value["reference"] != reference.fields()
            or not isinstance(value["result"], str)
        ):
            raise ValueError
        result = decode_response(
            value["result"], hashlib.sha256(value["result"].encode()).hexdigest()
        )
        if encode(reference, result) != raw:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, UnicodeError):
        raise ValueError("Invalid original serial response evidence") from None


async def read_serial_response(
    db: AsyncSession,
    reference: SerialResponseReference,
) -> EvaluationResponse | None:
    row = (
        (
            await db.execute(
                select(evaluation_response_receipts).where(
                    evaluation_response_receipts.c.run_id == str(reference.run_id),
                    evaluation_response_receipts.c.invocation_id
                    == reference.checkpoint.invocation_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["execution_id"], row["claim_version"]) != (
        reference.checkpoint.execution_id,
        reference.version,
    ):
        raise CheckpointConflict("Serial response ownership differs")
    return decode(reference, row["definition_json"], row["sha256"])


async def save_serial_response(
    db: AsyncSession,
    reference: SerialResponseReference,
    result: EvaluationResponse,
) -> None:
    """Caller commits only response evidence; does not advance a Run, invoke MQ or a model."""
    raw = encode(reference, result)
    if not isinstance(db, AsyncSession) or db.in_nested_transaction():
        raise BindingError("Original serial response root required")
    original = db.get_transaction()
    if original is None or not original.is_active:
        raise BindingError("Active original serial response root required")
    binding = bind(db)
    await binding.validate()
    cp = (
        (
            await db.execute(
                select(evaluation_checkpoints)
                .where(evaluation_checkpoints.c.run_id == str(reference.run_id))
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    run = (
        (
            await db.execute(
                select(evaluation_runs)
                .where(
                    evaluation_runs.c.run_id == str(reference.run_id),
                    evaluation_runs.c.organization_id == reference.organization_id,
                )
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if run is None or run["execution_mode"] != "serial_v1":
        raise CheckpointConflict("Original serial Run in organization required")
    existing = await read_serial_response(db, reference)
    if existing is not None:
        if existing.encode() != result.encode():
            raise CheckpointConflict("Immutable serial response already recorded")
        await binding.validate()
        return
    if cp is None or decode_checkpoint(cp["checkpoint_json"]) != reference.checkpoint:
        raise CheckpointConflict("Original dispatched serial checkpoint required")
    # Original serial completion also permits only this exact cancellation-drain
    # version transition. No new retry/attempt or lease is created here.
    if cp["version"] != reference.version:
        cancellation = (run["progress_json"] or {}).get("cancel_requested", {})
        if (cancellation.get("source_version"), cancellation.get("version")) != (
            reference.version,
            cp["version"],
        ):
            raise CheckpointConflict("Original serial response version changed")
    ledger = await db.scalar(
        select(evaluation_dispatches.c.checkpoint_json)
        .where(
            evaluation_dispatches.c.run_id == str(reference.run_id),
            evaluation_dispatches.c.invocation_id == reference.checkpoint.invocation_id,
        )
        .with_for_update()
    )
    if decode_checkpoint(ledger) != reference.checkpoint:
        raise CheckpointConflict("Exact original serial dispatch ledger required")
    await db.execute(
        insert(evaluation_response_receipts).values(
            run_id=str(reference.run_id),
            invocation_id=reference.checkpoint.invocation_id,
            execution_id=reference.checkpoint.execution_id,
            claim_version=reference.version,
            definition_json=raw,
            sha256=hashlib.sha256(raw.encode()).hexdigest(),
        )
    )
    if db.get_transaction() is not original or not original.is_active:
        raise BindingError("Original serial response root was replaced")
    await binding.validate()
