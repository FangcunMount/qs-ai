"""Frozen original preflight evidence; no model or automatic business retry."""

import asyncio
import hashlib
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import stored_run_suite
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import (
    complete_preflight,
    plan_preflight_completion,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state

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


async def load_preflight_source(
    db: AsyncSession, intent: EvaluationStateIntent, *, lock: bool
) -> tuple[Mapping[Any, Any], Mapping[Any, Any]]:
    checkpoint_query = select(evaluation_checkpoints).where(
        evaluation_checkpoints.c.run_id == intent.run_id
    )
    run_query = select(evaluation_runs).where(
        evaluation_runs.c.run_id == intent.run_id,
        evaluation_runs.c.organization_id == int(intent.organization_id),
    )
    if lock:
        checkpoint_query, run_query = (
            checkpoint_query.with_for_update(),
            run_query.with_for_update(),
        )
    checkpoint = (await db.execute(checkpoint_query)).mappings().one_or_none()
    if (
        checkpoint is None
        or checkpoint["version"] != intent.expected_version
        or checkpoint["checkpoint_json"] is not None
    ):
        raise CheckpointConflict("Run version changed or execution is active")
    run = (await db.execute(run_query)).mappings().one_or_none()
    if run is None:
        raise CheckpointConflict("Run unavailable in organization")
    return checkpoint, run


def _clock() -> datetime:
    return datetime.now(UTC)


def _identity() -> str:
    return str(uuid4())


class PreflightStatePlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        clock: Callable[[], datetime] = _clock,
        new_id: Callable[[], str] = _identity,
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive preflight database budget required")
        self._transactions, self._database_seconds = transactions, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key
        self._clock, self._new_id = clock, new_id

    async def candidate(self, intent: EvaluationStateIntent) -> FrozenPlan:
        if intent.operation_key != f"preflight/{intent.expected_version}":
            raise ValueError("Original worker preflight operation required")
        at = self._clock()
        async with asyncio.timeout(self._database_seconds), self._transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            checkpoint, run = await load_preflight_source(db, intent, lock=False)
            creation = json.loads(run["definition_json"])
            suite = await stored_run_suite(db, creation)
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == intent.run_id
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        # CPU-only preflight, IDs and first Seal follow read-transaction cleanup.
        evidence = await asyncio.to_thread(
            run_preflight, FrozenContractRef(**creation["release"]["suite"]), at, frozen_suite=suite
        )
        state, progress = plan_preflight_completion(
            UUID(intent.run_id),
            intent.expected_version,
            int(intent.organization_id),
            evidence,
            checkpoint,
            run,
        )
        operation = PreflightOperation(source_snapshot_digest(checkpoint, run), evidence)
        facts = EvaluationStateFacts(
            intent,
            at,
            previous["sequence"] if previous else 0,
            previous["version"] if previous else 0,
            operation.encode(),
        )
        visible = evaluation_state(
            intent.run_id,
            {**run, "progress_json": progress, "version": state.version},
            facts.sequence,
        )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(UUID(intent.run_id), f"state:{state.version}")),
            intent.run_id,
            pb.MessagingBody(evaluation_state=visible),
            organization_id=intent.organization_id,
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        return freeze_evaluation_state_plan(
            facts, self._new_id(), first, route_resolver=self._route_resolver
        )


async def apply_preflight_state(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    """Use only persisted winner evidence; host still owns this active root."""
    original = PreflightOperation.from_facts(facts)
    checkpoint, run = await load_preflight_source(db, facts.intent, lock=True)
    if source_snapshot_digest(checkpoint, run) != original.snapshot_digest:
        raise MessageConflict("Original preflight source snapshot changed")
    await complete_preflight(
        db,
        UUID(facts.intent.run_id),
        facts.intent.expected_version,
        int(facts.intent.organization_id),
        original.evidence,
    )
