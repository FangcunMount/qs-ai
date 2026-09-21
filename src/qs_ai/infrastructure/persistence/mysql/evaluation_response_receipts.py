"""Commit provider evidence independently from candidate/Run projection."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    lock_run,
    require_claim,
    terminal_dispatches,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_response_receipts as receipts

_FAILURE = TypeAdapter(ClassifiedFailure)
_CODEC = JSONModelCallCodec()


@dataclass(frozen=True)
class EvaluationResponse:
    response: ModelResponse | None
    failure: ClassifiedFailure | None
    finished_at: datetime

    def __post_init__(self) -> None:
        if (self.response is None) == (
            self.failure is None
        ) or self.finished_at.utcoffset() is None:
            raise ValueError(
                "Exactly one response or classified failure with UTC evidence required"
            )

    def encode(self) -> str:
        return json.dumps(
            {
                "schema_version": "qs-ai-evaluation-response/v1",
                "response": _CODEC.encode_response(self.response) if self.response else None,
                "failure": _FAILURE.dump_json(self.failure).decode() if self.failure else None,
                "finished_at": self.finished_at.isoformat(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def decode(raw: str, digest: str) -> EvaluationResponse:
    if hashlib.sha256(raw.encode()).hexdigest() != digest:
        raise ValueError("Evaluation response digest mismatch")
    value = json.loads(raw)
    if (
        set(value) != {"schema_version", "response", "failure", "finished_at"}
        or value["schema_version"] != "qs-ai-evaluation-response/v1"
    ):
        raise ValueError("Invalid evaluation response version")
    return EvaluationResponse(
        _CODEC.decode_response(value["response"]) if value["response"] is not None else None,
        _FAILURE.validate_json(value["failure"], strict=True)
        if value["failure"] is not None
        else None,
        datetime.fromisoformat(value["finished_at"]),
    )


async def read_response(db: AsyncSession, claim: SlotClaim) -> EvaluationResponse | None:
    row = (
        (
            await db.execute(
                select(receipts).where(
                    receipts.c.run_id == str(claim.run_id),
                    receipts.c.invocation_id == claim.checkpoint.invocation_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["execution_id"], row["claim_version"]) != (
        claim.checkpoint.execution_id,
        claim.version,
    ):
        raise CheckpointConflict("Response identity differs from claim")
    return decode(row["definition_json"], row["sha256"])


async def save_response(
    db: AsyncSession,
    organization_id: int,
    claim: SlotClaim,
    result: EvaluationResponse,
    *,
    at: datetime,
) -> None:
    """Caller commits. Duplicate identical evidence is safe; no terminal evidence is replaced."""
    await lock_run(db, claim.run_id, organization_id)
    current = await require_claim(db, claim)
    cp = current.checkpoint
    if cp.phase != "dispatching" or result.finished_at < cp.claimed_at:
        raise CheckpointConflict("Response requires a dispatched claim and valid finish time")
    if result.response is not None and result.response.invocation_id != cp.invocation_id:
        raise CheckpointConflict("Response invocation differs from claim")
    ledger = (
        (
            await db.execute(
                select(evaluation_dispatches)
                .where(
                    evaluation_dispatches.c.run_id == str(claim.run_id),
                    evaluation_dispatches.c.invocation_id == cp.invocation_id,
                )
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    terminal_dispatches(list(ledger), [], (current,))
    existing = await read_response(db, claim)
    if existing is not None:
        if existing.encode() != result.encode():
            raise CheckpointConflict("Immutable response already recorded")
        return
    # Expired ownership must be resolved by recovery; a late owner cannot create new evidence.
    if at.utcoffset() is None or not result.finished_at <= at < cp.lease_expires_at:
        raise CheckpointConflict("Response arrived after lease expiry")
    raw = result.encode()
    await db.execute(
        insert(receipts).values(
            run_id=str(claim.run_id),
            invocation_id=cp.invocation_id,
            execution_id=cp.execution_id,
            claim_version=claim.version,
            definition_json=raw,
            sha256=hashlib.sha256(raw.encode()).hexdigest(),
        )
    )
