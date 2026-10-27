"""Freeze original candidate selection and dispatch before the guarded business root."""

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import schema_assets
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import (
    candidate_checkpoint,
    candidate_plan,
    prepare_candidate,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, encode
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import plan_dispatch_record
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    active_claims,
    dispatch_claim,
    lock_run,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import (
    PreparedStep,
    StepInputs,
    load_step_inputs,
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
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
    inputs_digest,
    worker_source_digest,
)

_MAGIC = b"qs-ai-candidate-dispatch/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate candidate dispatch fact")
        value[key] = item
    return value


def candidate_dispatch_intent(
    run_id: UUID, version: int, organization_id: int
) -> EvaluationStateIntent:
    if (
        not isinstance(run_id, UUID)
        or type(version) is not int
        or not 1 <= version <= 2**63 - 2
        or type(organization_id) is not int
        or organization_id < 1
    ):
        raise ValueError("Original bounded candidate coordinator scope required")
    return EvaluationStateIntent(
        str(organization_id),
        str(run_id),
        f"candidate-dispatch/{version}",
        version,
        hashlib.sha256(
            b"qs-ai-candidate-dispatch-input/v1\0" + _json([str(run_id), version, organization_id])
        ).digest(),
    )


@dataclass(frozen=True)
class CandidateDispatchOperation:
    checkpoint: ExecutionCheckpoint = field(repr=False)
    limit: int
    source_hash: bytes = field(repr=False)
    inputs_hash: bytes = field(repr=False)
    record_hash: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.checkpoint, ExecutionCheckpoint)
            or self.checkpoint.phase != "prepared"
            or type(self.limit) is not int
            or not 1 <= self.limit <= 32
            or any(
                type(v) is not bytes or len(v) != 32
                for v in (self.source_hash, self.inputs_hash, self.record_hash)
            )
        ):
            raise ValueError("Exact bounded original candidate dispatch required")

    def encode(self) -> bytes:
        raw = _MAGIC + _json(
            dict(
                checkpoint=encode(self.checkpoint),
                limit=self.limit,
                source_hash=self.source_hash.hex(),
                inputs_hash=self.inputs_hash.hex(),
                record_hash=self.record_hash.hex(),
            )
        )
        if len(raw) > 8192:
            raise ValueError("Candidate dispatch context exceeds bound")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "CandidateDispatchOperation":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded candidate dispatch context required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(value, dict) or set(value) != {
                "checkpoint",
                "limit",
                "source_hash",
                "inputs_hash",
                "record_hash",
            }:
                raise ValueError
            cp = decode(value["checkpoint"])
            if cp is None:
                raise ValueError
            hashes = [value[key] for key in ("source_hash", "inputs_hash", "record_hash")]
            if any(
                not isinstance(v, str) or re.fullmatch("[0-9a-f]{64}", v) is None for v in hashes
            ):
                raise ValueError
            result = cls(cp, value["limit"], *(bytes.fromhex(v) for v in hashes))
            if result.encode() != raw:
                raise ValueError
            return result
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise ValueError("Invalid first candidate dispatch context") from error


def read_candidate_dispatch_plan(
    intent: EvaluationStateIntent,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, CandidateDispatchOperation, FrozenArchive]:
    facts, archive = read_evaluation_state_plan(intent, winner, route_resolver=route_resolver)
    operation = CandidateDispatchOperation.decode(facts.business_context)
    expected = candidate_dispatch_intent(
        UUID(intent.run_id), intent.expected_version, int(intent.organization_id)
    )
    visible = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        expected != intent
        or facts.source_version is not None
        or visible.version != intent.expected_version + 1
    ):
        raise MessageConflict("Original candidate dispatch scope or single CAS differs")
    return facts, operation, archive


def _source_hash(run_id: UUID, run: dict, version: int, active: tuple) -> bytes:
    claims = []
    for claim in active:
        cp = encode(claim.checkpoint)
        assert cp is not None
        # Heartbeat alone does not change next-slot selection or confer model authority.
        cp.pop("lease_expires_at", None)
        claims.append([str(claim.run_id), claim.version, cp])
    return hashlib.sha256(
        b"qs-ai-candidate-dispatch-source/v1\0"
        + worker_source_digest(dict(run_id=str(run_id), version=version, checkpoint_json=None), run)
        + _json(claims)
    ).digest()


class CandidateDispatchPlanner:
    def __init__(
        self,
        transactions: Transactions,
        schemas: SchemaAssets,
        *,
        signing_key: Any,
        recipient_key: Any,
        limit: int,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        new_id: Callable[[], str] = lambda: str(uuid4()),
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if (
            not math.isfinite(database_seconds)
            or database_seconds <= 0
            or type(limit) is not int
            or not 1 <= limit <= 32
        ):
            raise ValueError("Finite candidate database and concurrency bounds required")
        self.transactions, self.schemas = transactions, schemas
        self.signing_key, self.recipient_key, self.limit = signing_key, recipient_key, limit
        self.clock, self.new_id, self.database_seconds = clock, new_id, database_seconds

    async def candidate(
        self, run_id: UUID, version: int, organization_id: int, owner: str
    ) -> tuple[FrozenPlan, StepInputs]:
        intent = candidate_dispatch_intent(run_id, version, organization_id)
        if (
            not isinstance(owner, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", owner) is None
        ):
            raise ValueError("Bounded original candidate worker owner required")
        at, execution_id, invocation_id = self.clock(), self.new_id(), self.new_id()
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            run, current = await lock_run(db, run_id, organization_id, lock=False)
            if current != version:
                raise CheckpointConflict("Candidate coordinator version changed")
            active = await active_claims(db, run_id, lock=False)
            plan = await candidate_plan(db, run_id, run, active, self.limit, lock=False)
            cp = candidate_checkpoint(
                run, plan, owner, execution_id, invocation_id, at, at + timedelta(minutes=5)
            )
            inputs = await load_step_inputs(
                db, run_id, organization_id, cp, AssetSnapshotReader(db, schema_assets, SchemaAsset)
            )
            record = await plan_dispatch_record(db, run_id, cp.mark_dispatching(owner, at))
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            operation = CandidateDispatchOperation(
                cp,
                self.limit,
                _source_hash(run_id, run, version, active),
                inputs_digest(inputs),
                hashlib.sha256(record.record_json.encode()).digest(),
            )
            facts = EvaluationStateFacts(
                intent,
                datetime.fromisoformat(run["progress_json"]["transitions"][-1]["at"]),
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                operation.encode(),
            )
            visible = evaluation_state(str(run_id), {**run, "version": version + 1}, facts.sequence)
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(run_id, f"state:{version + 1}")),
            str(run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(organization_id),
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(
            facts, self.new_id(), first, route_resolver=self._route_resolver
        )
        read_candidate_dispatch_plan(intent, winner, route_resolver=self._route_resolver)
        return winner, inputs


class CandidateDispatchWrite:
    def __init__(self) -> None:
        self.prepared: PreparedStep | None = None

    async def __call__(self, db: AsyncSession, facts: EvaluationStateFacts) -> None:
        intent = facts.intent
        operation = CandidateDispatchOperation.decode(facts.business_context)
        cp = operation.checkpoint
        run_id, organization_id = UUID(intent.run_id), int(intent.organization_id)
        run, version = await lock_run(db, run_id, organization_id)
        active = await active_claims(db, run_id)
        if (
            version != intent.expected_version
            or _source_hash(run_id, run, version, active) != operation.source_hash
        ):
            raise MessageConflict("Original candidate dispatch source changed")
        claim = await prepare_candidate(
            db,
            run_id,
            organization_id,
            cp.owner,
            cp.execution_id,
            cp.invocation_id,
            cp.claimed_at,
            cp.lease_expires_at,
            limit=operation.limit,
        )
        if claim.checkpoint != cp or claim.version != version + 1:
            raise MessageConflict("Original candidate selection differs from frozen first action")
        inputs = await load_step_inputs(
            db, run_id, organization_id, cp, AssetSnapshotReader(db, schema_assets, SchemaAsset)
        )
        if inputs_digest(inputs) != operation.inputs_hash:
            raise MessageConflict("Original candidate input changed")
        record = await plan_dispatch_record(
            db, run_id, cp.mark_dispatching(cp.owner, cp.claimed_at), lock=True
        )
        if hashlib.sha256(record.record_json.encode()).digest() != operation.record_hash:
            raise MessageConflict("Original candidate dispatch ledger changed")
        claim = await dispatch_claim(db, organization_id, claim, cp.claimed_at)
        dispatched = claim.checkpoint
        self.prepared = PreparedStep(
            CheckpointState(run_id, claim.version, dispatched),
            dispatched,
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
            claim,
        )
