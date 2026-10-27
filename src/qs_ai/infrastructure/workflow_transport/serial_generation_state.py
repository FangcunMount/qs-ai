"""First durable serial response -> frozen terminal state -> original guarded root.

No provider call, candidate SlotClaim, autonomous retry or publication scan lives here.
"""

import asyncio
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.completion import validate_generation_completion
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import route_assets, schema_assets
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, encode
from qs_ai.infrastructure.persistence.mysql.evaluation_completion_progress import (
    generation_completion_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import (
    SerialGenerationSource,
    complete_evaluated_generation,
    load_serial_generation_source,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_frozen_policies import frozen_policies
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    SerialResponseReference,
    read_serial_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import (
    PreparedStep,
    StepCompletion,
    load_step_inputs,
    prepare_step_completion,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
    inputs_digest,
    worker_source_digest,
)

_MAGIC = b"qs-ai-serial-generation-completion/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate serial generation fact")
        result[key] = value
    return result


def response_digest(result: EvaluationResponse) -> bytes:
    return hashlib.sha256(result.encode().encode()).digest()


def completion_digest(value: GenerationCompletion) -> bytes:
    fields = asdict(value)
    for key in ("started_at", "finished_at"):
        fields[key] = fields[key].isoformat()
    for key in ("raw_output", "normalized_output"):
        fields[key] = hashlib.sha256(fields[key]).hexdigest()
    return hashlib.sha256(b"qs-ai-serial-generation-evidence/v1\0" + _json(fields)).digest()


def serial_generation_intent(
    reference: SerialResponseReference, digest: bytes
) -> EvaluationStateIntent:
    if (
        not isinstance(reference, SerialResponseReference)
        or reference.checkpoint.kind != "generation"
        or reference.version >= 2**63 - 1
        or type(digest) is not bytes
        or len(digest) != 32
    ):
        raise ValueError("Exact original serial generation response identity required")
    # Full original reference and response bind input; generated candidate/time/source do not.
    return EvaluationStateIntent(
        str(reference.organization_id),
        str(reference.run_id),
        "serial-generation/"
        + hashlib.sha256(reference.checkpoint.invocation_id.encode()).hexdigest(),
        reference.version,
        hashlib.sha256(
            b"qs-ai-serial-generation-input/v1\0" + _json([reference.fields(), digest.hex()])
        ).digest(),
    )


@dataclass(frozen=True)
class SerialGenerationOperation:
    reference: SerialResponseReference = field(repr=False)
    response_hash: bytes = field(repr=False)
    source_hash: bytes = field(repr=False)
    inputs_hash: bytes = field(repr=False)
    completion_hash: bytes = field(repr=False)
    candidate_id: str = field(repr=False)

    def __post_init__(self) -> None:
        serial_generation_intent(self.reference, self.response_hash)
        if (
            any(
                type(v) is not bytes or len(v) != 32
                for v in (self.source_hash, self.inputs_hash, self.completion_hash)
            )
            or type(self.candidate_id) is not str
            or (
                self.candidate_id
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.candidate_id) is None
            )
        ):
            raise ValueError("Exact first serial generation completion facts required")

    @property
    def intent(self) -> EvaluationStateIntent:
        return serial_generation_intent(self.reference, self.response_hash)

    def encode(self) -> bytes:
        value = dict(reference=self.reference.fields(), candidate_id=self.candidate_id)
        value.update(
            {
                k: getattr(self, k).hex()
                for k in ("response_hash", "source_hash", "inputs_hash", "completion_hash")
            }
        )
        raw = _MAGIC + _json(value)
        if len(raw) > 8192:
            raise ValueError("Serial generation facts exceed context bound")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "SerialGenerationOperation":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded serial generation facts required")
        try:
            value = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            hashes = ("response_hash", "source_hash", "inputs_hash", "completion_hash")
            if not isinstance(value, dict) or set(value) != {"reference", "candidate_id", *hashes}:
                raise ValueError
            ref = value["reference"]
            if not isinstance(ref, dict) or set(ref) != {
                "run_id",
                "organization_id",
                "version",
                "checkpoint",
            }:
                raise ValueError
            cp = decode(ref["checkpoint"])
            if cp is None:
                raise ValueError
            result = cls(
                SerialResponseReference(
                    UUID(ref["run_id"]), ref["organization_id"], ref["version"], cp
                ),
                bytes.fromhex(value["response_hash"]),
                bytes.fromhex(value["source_hash"]),
                bytes.fromhex(value["inputs_hash"]),
                bytes.fromhex(value["completion_hash"]),
                value["candidate_id"],
            )
            if result.encode() != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen serial generation completion facts") from None


def read_serial_generation_plan(
    intent: EvaluationStateIntent,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, SerialGenerationOperation]:
    facts, archive = read_evaluation_state_plan(intent, winner, route_resolver=route_resolver)
    operation = SerialGenerationOperation.decode(facts.business_context)
    state = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        operation.intent != intent
        or facts.source_version is None
        or state.version != facts.source_version + 1
    ):
        raise MessageConflict(
            "Original serial generation identity or exact terminal version differs"
        )
    return facts, operation


@dataclass(frozen=True)
class _Loaded:
    operation: SerialGenerationOperation
    value: StepCompletion = field(repr=False)
    source: SerialGenerationSource = field(repr=False)
    progress: dict = field(repr=False)


async def _load(
    db: AsyncSession,
    reference: SerialResponseReference,
    routes: RouteAssets,
    *,
    candidate_id: str | None,
    lock: bool,
) -> _Loaded:
    serial_generation_intent(reference, bytes(32))
    # This is the separately committed original response, never a fresh provider invocation.
    result = await read_serial_response(db, reference)
    if result is None:
        raise CheckpointConflict("Original durable serial response required before finalization")
    serial_generation_intent(reference, response_digest(result))
    cp = reference.checkpoint
    assert cp.dispatch_started_at is not None
    initial = replace(cp, phase="prepared", dispatch_started_at=None)
    schemas = AssetSnapshotReader(db, schema_assets, SchemaAsset)
    inputs = await load_step_inputs(
        db, reference.run_id, reference.organization_id, initial, schemas
    )
    prepared = PreparedStep(
        CheckpointState(reference.run_id, reference.version, cp),
        initial,
        cp.dispatch_started_at,
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
    value = await prepare_step_completion(
        prepared,
        result.response,
        result.failure,
        routes,
        clock=lambda: result.finished_at,
        candidate_id=candidate_id,
    )
    completion = value.completion
    if not isinstance(completion, GenerationCompletion):
        raise MessageConflict("Serial generation completion kind required")
    source = await load_serial_generation_source(
        db,
        reference.run_id,
        reference.version,
        reference.organization_id,
        cp.owner,
        completion,
        lock=lock,
    )
    if source.run["execution_mode"] != "serial_v1":
        raise CheckpointConflict("Serial completion cannot impersonate candidate ownership")
    ledger_query = select(evaluation_dispatches.c.checkpoint_json).where(
        evaluation_dispatches.c.run_id == str(reference.run_id),
        evaluation_dispatches.c.invocation_id == cp.invocation_id,
    )
    if lock:
        ledger_query = ledger_query.with_for_update()
    ledger = decode(await db.scalar(ledger_query))
    if ledger is None or not completion.matches_checkpoint(ledger, cp.owner):
        raise CheckpointConflict("Original immutable serial dispatch ledger required")
    snapshot_routes = AssetSnapshotReader(db, route_assets, RouteAsset)
    await validate_generation_completion(inputs.release, completion, snapshot_routes, schemas)
    creation = json.loads(source.run["definition_json"])
    policy, _ = frozen_policies(creation)
    if creation["execution_policy_json"] != policy.definition_json:
        raise CheckpointConflict("Unsupported frozen execution policy")
    progress = generation_completion_progress(
        source.run["progress_json"], completion, policy, cp.owner, candidate_claim=False
    )
    source_hash = worker_source_digest(
        dict(
            run_id=str(reference.run_id),
            version=source.version,
            checkpoint_json=encode(source.checkpoint),
        ),
        source.run,
    )
    operation = SerialGenerationOperation(
        reference,
        response_digest(result),
        source_hash,
        inputs_digest(inputs),
        completion_digest(completion),
        value.candidate_id,
    )
    return _Loaded(operation, value, source, progress)


class SerialGenerationPlanner:
    def __init__(
        self,
        transactions: Transactions,
        routes: RouteAssets,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive serial completion database budget required")
        self.transactions, self.routes = transactions, routes
        self.signing_key, self.recipient_key = signing_key, recipient_key
        self.database_seconds = database_seconds

    async def intent(self, reference: SerialResponseReference) -> EvaluationStateIntent:
        serial_generation_intent(reference, bytes(32))
        # Reads remain valid after terminal projection, so duplicates find their first plan.
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            result = await read_serial_response(db, reference)
            if result is None:
                raise CheckpointConflict("Durable original serial response missing")
            return serial_generation_intent(reference, response_digest(result))

    async def candidate(
        self, reference: SerialResponseReference, *, candidate_id: str | None = None
    ) -> FrozenPlan:
        serial_generation_intent(reference, bytes(32))
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            loaded = await _load(db, reference, self.routes, candidate_id=candidate_id, lock=False)
            operation, source = loaded.operation, loaded.source
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(reference.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            transitions = loaded.progress["transitions"]
            at = datetime.fromisoformat(transitions[-1]["at"])
            facts = EvaluationStateFacts(
                operation.intent,
                at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                operation.encode(),
                source.version,
            )
            visible = evaluation_state(
                str(reference.run_id),
                {**source.run, "progress_json": loaded.progress, "version": source.version + 1},
                facts.sequence,
            )
        # All original reads are closed before signing/encryption and all half messages.
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(reference.run_id, f"state:{source.version + 1}")),
            str(reference.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(reference.organization_id),
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(
            facts, str(uuid4()), first, route_resolver=self._route_resolver
        )
        read_serial_generation_plan(operation.intent, winner, route_resolver=self._route_resolver)
        return winner

    async def verify(self, reference: SerialResponseReference, winner: FrozenPlan) -> None:
        intent = await self.intent(reference)
        facts, expected = read_serial_generation_plan(
            intent, winner, route_resolver=self._route_resolver
        )
        if expected.reference != reference:
            raise MessageConflict("Original serial response reference changed")
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            loaded = await _load(
                db, reference, self.routes, candidate_id=expected.candidate_id, lock=False
            )
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(reference.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            actual_sequence = (previous["sequence"], previous["version"]) if previous else (0, 0)
            if (
                loaded.operation != expected
                or loaded.source.version != facts.source_version
                or actual_sequence != (facts.previous_sequence, facts.previous_version)
            ):
                raise MessageConflict("First serial completion source or sequence changed")


class SerialGenerationWrite:
    """Original terminal acceptor stages business; outer root commits archive and witness."""

    def __init__(self, routes: RouteAssets) -> None:
        self.routes = routes
        self.state: CheckpointState | None = None

    async def __call__(self, db: AsyncSession, facts: EvaluationStateFacts) -> None:
        expected = SerialGenerationOperation.decode(facts.business_context)
        if expected.intent != facts.intent:
            raise MessageConflict("Original serial completion operation differs")
        loaded = await _load(
            db, expected.reference, self.routes, candidate_id=expected.candidate_id, lock=True
        )
        if loaded.operation != expected or loaded.source.version != facts.source_version:
            raise MessageConflict("Original serial completion source or frozen evidence changed")
        value, ref = loaded.value.completion, expected.reference
        assert isinstance(value, GenerationCompletion)
        self.state = await complete_evaluated_generation(
            db,
            ref.run_id,
            ref.version,
            ref.organization_id,
            ref.checkpoint.owner,
            value,
            AssetSnapshotReader(db, route_assets, RouteAsset),
            AssetSnapshotReader(db, schema_assets, SchemaAsset),
            candidate_id=expected.candidate_id,
        )
        if self.state.version != loaded.source.version + 1 or self.state.checkpoint is not None:
            raise MessageConflict("Original serial terminal result differs from first projection")
