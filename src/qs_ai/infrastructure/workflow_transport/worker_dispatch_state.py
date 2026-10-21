"""Serial prepare + dispatch in one original guarded root; never invokes a model."""

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import schema_assets
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, encode
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import (
    plan_dispatch_record,
    reserve_dispatch,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import (
    plan_prepare_execution,
    prepare_execution,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import (
    PreparedStep,
    StepInputs,
    load_step_inputs,
)
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    load_preflight_source,
    source_snapshot_digest,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive

_MAGIC = b"qs-ai-worker-dispatch/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate original worker dispatch fact")
        value[key] = item
    return value


def inputs_digest(inputs: StepInputs) -> bytes:
    # Includes exact message strings, model identity, schema, assertions and frozen assets.
    return hashlib.sha256(b"qs-ai-step-inputs/v1\0" + _json(asdict(inputs))).digest()


def worker_dispatch_intent(
    run_id: UUID, version: int, organization_id: int, owner: str
) -> EvaluationStateIntent:
    if (
        not isinstance(run_id, UUID)
        or type(organization_id) is not int
        or not isinstance(owner, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", owner) is None
        or type(version) is not int
        or not 1 <= version <= 2**63 - 3
    ):
        raise ValueError("Original bounded serial worker scope required")
    # Mutable sources and first UUIDs/time do not participate in request identity.
    return EvaluationStateIntent(
        str(organization_id),
        str(run_id),
        f"worker-dispatch/{version}",
        version,
        hashlib.sha256(
            b"qs-ai-worker-dispatch-input/v1\0" + _json([str(run_id), version, organization_id])
        ).digest(),
    )


def worker_source_digest(checkpoint: Any, run: Any) -> bytes:
    return hashlib.sha256(
        b"qs-ai-worker-source/v1\0"
        + source_snapshot_digest(checkpoint, run)
        + _json(
            [
                run["execution_mode"],
                run["frozen_execution_policy_fingerprint"],
                run["frozen_execution_policy_json"],
            ]
        )
    ).digest()


@dataclass(frozen=True)
class WorkerDispatchOperation:
    checkpoint: ExecutionCheckpoint = field(repr=False)
    source_digest: bytes = field(repr=False)
    model_inputs_digest: bytes = field(repr=False)
    record_digest: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.checkpoint, ExecutionCheckpoint)
            or self.checkpoint.phase != "prepared"
            or any(
                type(v) is not bytes or len(v) != 32
                for v in (self.source_digest, self.model_inputs_digest, self.record_digest)
            )
        ):
            raise ValueError("Exact original worker dispatch facts required")

    def encode(self) -> bytes:
        raw = _MAGIC + _json(
            dict(
                checkpoint=encode(self.checkpoint),
                source_digest=self.source_digest.hex(),
                model_inputs_digest=self.model_inputs_digest.hex(),
                record_digest=self.record_digest.hex(),
            )
        )
        if len(raw) > 8192:
            raise ValueError("Original worker dispatch facts exceed bounds")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "WorkerDispatchOperation":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded worker dispatch facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {
                "checkpoint",
                "source_digest",
                "model_inputs_digest",
                "record_digest",
            }:
                raise ValueError
            cp = decode(value["checkpoint"])
            if cp is None:
                raise ValueError
            result = cls(
                cp,
                *(
                    bytes.fromhex(value[k])
                    for k in ("source_digest", "model_inputs_digest", "record_digest")
                ),
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original worker dispatch facts") from None

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> "WorkerDispatchOperation":
        intent = facts.intent
        result = cls.decode(facts.business_context)
        if (
            worker_dispatch_intent(
                UUID(intent.run_id),
                intent.expected_version,
                int(intent.organization_id),
                result.checkpoint.owner,
            )
            != intent
        ):
            raise ValueError("Original worker identity differs from frozen dispatch")
        return result


def read_worker_dispatch_plan(
    intent: EvaluationStateIntent, winner: FrozenPlan
) -> tuple[EvaluationStateFacts, WorkerDispatchOperation, FrozenArchive]:
    facts, archive = read_evaluation_state_plan(intent, winner)
    operation = WorkerDispatchOperation.from_facts(facts)
    state = pb.MessagingBody.FromString(archive.body).evaluation_state
    if state.version != intent.expected_version + 2 or state.status != "collecting":
        raise MessageConflict("Original two-CAS worker dispatch projection required")
    return facts, operation, archive


class WorkerDispatchPlanner:
    def __init__(
        self,
        transactions: Transactions,
        schemas: SchemaAssets,
        *,
        signing_key: Any,
        recipient_key: Any,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        new_id: Callable[[], str] = lambda: str(uuid4()),
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive worker database budget required")
        self.transactions, self.schemas = transactions, schemas
        self.signing_key, self.recipient_key = signing_key, recipient_key
        self.clock, self.new_id, self.database_seconds = clock, new_id, database_seconds

    async def candidate(
        self, run_id: UUID, version: int, organization_id: int, owner: str
    ) -> tuple[FrozenPlan, StepInputs]:
        intent = worker_dispatch_intent(run_id, version, organization_id, owner)
        at, execution_id, invocation_id = self.clock(), self.new_id(), self.new_id()
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            checkpoint, run = await load_preflight_source(db, intent, lock=False)
            state = await plan_prepare_execution(
                db,
                run_id,
                version,
                organization_id,
                owner,
                execution_id,
                invocation_id,
                at,
                at + timedelta(minutes=5),
            )
            cp = state.checkpoint
            assert cp is not None
            inputs = await load_step_inputs(
                db, run_id, organization_id, cp, AssetSnapshotReader(db, schema_assets, SchemaAsset)
            )
            record = await plan_dispatch_record(db, run_id, cp.mark_dispatching(owner, at))
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
            # Preserve the original evaluation event's last business-transition time.
            transitions = run["progress_json"].get("transitions", [])
            event_at = datetime.fromisoformat(transitions[-1]["at"])
            operation = WorkerDispatchOperation(
                cp,
                worker_source_digest(checkpoint, run),
                inputs_digest(inputs),
                hashlib.sha256(record.record_json.encode()).digest(),
            )
            facts = EvaluationStateFacts(
                intent,
                event_at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                operation.encode(),
            )
            visible = evaluation_state(
                intent.run_id, {**run, "version": version + 2}, facts.sequence
            )
        # No open source session, UUID generation or Seal in the original business callback.
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(run_id, f"state:{version + 2}")),
            intent.run_id,
            pb.MessagingBody(evaluation_state=visible),
            organization_id=intent.organization_id,
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(facts, self.new_id(), first)
        read_worker_dispatch_plan(intent, winner)
        return winner, inputs


class WorkerDispatchWrite:
    """Stages original preparation and dispatch; outer root owns commit and witness."""

    def __init__(self, schemas: SchemaAssets) -> None:
        self.schemas = schemas
        self.prepared: PreparedStep | None = None

    async def __call__(self, db: AsyncSession, facts: EvaluationStateFacts) -> None:
        operation = WorkerDispatchOperation.from_facts(facts)
        cp, intent = operation.checkpoint, facts.intent
        checkpoint, run = await load_preflight_source(db, intent, lock=True)
        if worker_source_digest(checkpoint, run) != operation.source_digest:
            raise MessageConflict("Original worker source changed")
        run_id, organization_id = UUID(intent.run_id), int(intent.organization_id)
        # Same original root and both original CAS operations; first identities are supplied.
        state = await prepare_execution(
            db,
            run_id,
            intent.expected_version,
            organization_id,
            cp.owner,
            cp.execution_id,
            cp.invocation_id,
            cp.claimed_at,
            cp.lease_expires_at,
        )
        if state.checkpoint != cp:
            raise MessageConflict("Original prepared checkpoint differs from first plan")
        inputs = await load_step_inputs(
            db, run_id, organization_id, cp, AssetSnapshotReader(db, schema_assets, SchemaAsset)
        )
        if inputs_digest(inputs) != operation.model_inputs_digest:
            raise MessageConflict("Original model input differs from first plan")
        record = await plan_dispatch_record(
            db, run_id, cp.mark_dispatching(cp.owner, cp.claimed_at), lock=True
        )
        if hashlib.sha256(record.record_json.encode()).digest() != operation.record_digest:
            raise MessageConflict("Original immutable dispatch record changed")
        state = await reserve_dispatch(db, run_id, state.version, cp.owner, cp.claimed_at)
        self.prepared = PreparedStep(
            state,
            cp,
            cp.claimed_at,
            cp.invocation_id,
            cp.execution_id,
            inputs.candidate_fingerprint,
            inputs.assertions,
            inputs.release,
            inputs.suite,
            inputs.route,
            inputs.messages,
            inputs.schema,
            inputs.max_output_characters,
            inputs.semantic,
        )
