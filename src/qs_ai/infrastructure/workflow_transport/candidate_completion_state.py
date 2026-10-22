"""First durable candidate response -> frozen terminal state -> original guarded root.

Exact existing candidate ownership and durable response.
No provider call, autonomous retry or publication scan.
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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import route_assets, schema_assets
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions,
    select_records,
    semantic_completions,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
    candidate_completion_progress,
    candidate_completion_target,
    completion_owner,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import (
    project_candidate_records,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_records import (
    generation_terminal_records,
    semantic_terminal_records,
    virtual_blocked_execution,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    CandidateResponseReference,
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode, encode
from qs_ai.infrastructure.persistence.mysql.evaluation_completion_progress import (
    generation_completion_progress,
    semantic_completion_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import (
    complete_evaluated_generation,
    evaluated_generation_assertions,
    generation_candidate,
    read_generation_acceptance,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import (
    complete_semantic,
    read_semantic_acceptance,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim, active_claims
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

_MAGIC = b"qs-ai-candidate-completion/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate candidate terminal fact")
        result[key] = value
    return result


def response_digest(result: EvaluationResponse) -> bytes:
    return hashlib.sha256(result.encode().encode()).digest()


def completion_digest(value: GenerationCompletion | SemanticCompletion) -> bytes:
    fields = asdict(value)
    for key in ("started_at", "finished_at"):
        fields[key] = fields[key].isoformat()
    for key in ("raw_output", "normalized_output"):
        fields[key] = hashlib.sha256(fields[key]).hexdigest()
    return hashlib.sha256(b"qs-ai-candidate-completion-evidence/v1\0" + _json(fields)).digest()


def candidate_completion_intent(
    reference: CandidateResponseReference, digest: bytes
) -> EvaluationStateIntent:
    if (
        not isinstance(reference, CandidateResponseReference)
        or reference.checkpoint.kind not in ("generation", "semantic")
        or reference.version >= 2**63 - 1
        or type(digest) is not bytes
        or len(digest) != 32
    ):
        raise ValueError("Exact original candidate terminal response identity required")
    # Full original reference and response bind input; generated candidate/time/source do not.
    return EvaluationStateIntent(
        str(reference.organization_id),
        str(reference.run_id),
        "candidate-completion/"
        + hashlib.sha256(reference.checkpoint.invocation_id.encode()).hexdigest(),
        reference.version,
        hashlib.sha256(
            b"qs-ai-candidate-completion-input/v1\0" + _json([reference.fields(), digest.hex()])
        ).digest(),
    )


@dataclass(frozen=True)
class CandidateCompletionOperation:
    reference: CandidateResponseReference = field(repr=False)
    response_hash: bytes = field(repr=False)
    source_hash: bytes = field(repr=False)
    inputs_hash: bytes = field(repr=False)
    completion_hash: bytes = field(repr=False)
    candidate_id: str = field(repr=False)

    def __post_init__(self) -> None:
        candidate_completion_intent(self.reference, self.response_hash)
        if (
            any(
                type(v) is not bytes or len(v) != 32
                for v in (self.source_hash, self.inputs_hash, self.completion_hash)
            )
            or (
                self.reference.checkpoint.kind == "semantic"
                and self.candidate_id != self.reference.checkpoint.candidate_id
            )
            or type(self.candidate_id) is not str
            or (
                self.candidate_id
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.candidate_id) is None
            )
        ):
            raise ValueError("Exact first candidate terminal completion facts required")

    @property
    def intent(self) -> EvaluationStateIntent:
        return candidate_completion_intent(self.reference, self.response_hash)

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
            raise ValueError("Candidate completion facts exceed context bound")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> "CandidateCompletionOperation":
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Explicit bounded candidate terminal facts required")
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
                CandidateResponseReference(
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
            raise ValueError("Invalid frozen candidate terminal completion facts") from None


def read_candidate_completion_plan(
    intent: EvaluationStateIntent, winner: FrozenPlan
) -> tuple[EvaluationStateFacts, CandidateCompletionOperation]:
    facts, archive = read_evaluation_state_plan(intent, winner)
    operation = CandidateCompletionOperation.decode(facts.business_context)
    state = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        operation.intent != intent
        or facts.source_version is None
        or state.version != facts.source_version + 1
    ):
        raise MessageConflict(
            "Original candidate terminal identity or exact terminal version differs"
        )
    return facts, operation


@dataclass(frozen=True)
class _CandidateSource:
    version: int
    run: dict = field(repr=False)
    claim: SlotClaim = field(repr=False)
    active: tuple[SlotClaim, ...] = field(repr=False)


@dataclass(frozen=True)
class _Loaded:
    operation: CandidateCompletionOperation
    value: StepCompletion = field(repr=False)
    source: _CandidateSource = field(repr=False)
    progress: dict = field(repr=False)


async def _response(
    db: AsyncSession, reference: CandidateResponseReference
) -> EvaluationResponse | None:
    found = await read_candidate_response(
        db,
        reference.organization_id,
        SlotClaim(reference.run_id, reference.version, reference.checkpoint),
    )
    if found is None:
        return None
    if found.reference != reference:
        raise MessageConflict("Canonical original candidate dispatch reference required")
    return found.result


def candidate_source_digest(run_id: UUID, source: _CandidateSource) -> bytes:
    claims = []
    for claim in source.active:
        cp = encode(claim.checkpoint)
        assert cp is not None
        # A completion consumes already durable response evidence. Renewal alone
        # changes no completion right; require_claim still fences version/owner/phase.
        cp.pop("lease_expires_at", None)
        claims.append([str(claim.run_id), claim.version, cp])
    return hashlib.sha256(
        b"qs-ai-candidate-terminal-source/v1\0"
        + worker_source_digest(
            dict(run_id=str(run_id), version=source.version, checkpoint_json=None), source.run
        )
        + _json(claims)
    ).digest()


async def _load(
    db: AsyncSession,
    reference: CandidateResponseReference,
    *,
    candidate_id: str | None,
    lock: bool,
) -> _Loaded:
    candidate_completion_intent(reference, bytes(32))
    cp = reference.checkpoint
    if cp.kind == "semantic" and candidate_id is not None and candidate_id != cp.candidate_id:
        raise ValueError("Original accepted semantic candidate identity required")
    result = await _response(db, reference)
    if result is None:
        raise CheckpointConflict("Original durable candidate response required before finalization")
    run, version, current = await completion_owner(
        db,
        reference.run_id,
        reference.organization_id,
        SlotClaim(reference.run_id, reference.version, cp),
        lock=lock,
    )
    if version >= 2**63 - 1:
        raise MessageConflict("Candidate coordinator version exhausted")
    source = _CandidateSource(
        version, run, current, await active_claims(db, reference.run_id, lock=lock)
    )
    assert cp.dispatch_started_at is not None
    initial = replace(cp, phase="prepared", dispatch_started_at=None)
    schemas = AssetSnapshotReader(db, schema_assets, SchemaAsset)
    routes = AssetSnapshotReader(db, route_assets, RouteAsset)
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
        SlotClaim(reference.run_id, reference.version, cp),
    )
    value = await prepare_step_completion(
        prepared,
        result.response,
        result.failure,
        routes,
        clock=lambda: result.finished_at,
        **({"candidate_id": candidate_id} if cp.kind == "generation" else {}),
    )
    completion = value.completion
    if isinstance(completion, GenerationCompletion):
        assertions = await evaluated_generation_assertions(
            db, reference.run_id, reference.organization_id, completion, schemas
        )
        proposed = generation_candidate(completion, value.candidate_id, assertions)
        gen = await read_generation_acceptance(
            db,
            reference.run_id,
            run,
            current.checkpoint,
            cp.owner,
            completion,
            routes,
            schemas,
            proposed,
            lock=lock,
        )
        policy, creation = gen.policy, gen.creation
        progress = generation_completion_progress(
            run["progress_json"], completion, policy, cp.owner, candidate_claim=True
        )
        first_candidate = value.candidate_id
    else:
        assert isinstance(completion, SemanticCompletion)
        sem = await read_semantic_acceptance(
            db,
            reference.run_id,
            reference.organization_id,
            cp.owner,
            completion,
            run,
            current.checkpoint,
            routes,
            lock=lock,
        )
        policy, creation = sem.policy, sem.creation
        progress = semantic_completion_progress(
            run["progress_json"], completion, policy, cp.owner, candidate_claim=True
        )
        first_candidate = cp.candidate_id
    records = []
    for table in (evaluation_dispatches, generation_completions, semantic_completions):
        statement = select_records(table).where(table.c.run_id == str(reference.run_id))
        if lock:
            statement = statement.with_for_update()
        records.append(list((await db.execute(statement)).mappings().all()))
    if isinstance(completion, GenerationCompletion):
        virtual = generation_terminal_records(
            reference.run_id,
            current,
            source.active,
            completion,
            gen,
            dispatches=records[0],
            generations=records[1],
            semantics=records[2],
        )
    else:
        virtual = semantic_terminal_records(
            reference.run_id,
            current,
            source.active,
            completion,
            sem,
            dispatches=records[0],
            generations=records[1],
            semantics=records[2],
        )
    plan = await project_candidate_records(
        creation,
        progress,
        policy,
        virtual.active,
        32,
        dispatches=list(virtual.dispatches),
        generations=list(virtual.generations),
        semantics=list(virtual.semantics),
    )
    target, cause = candidate_completion_target(progress, virtual.active, plan)
    blocked = (
        virtual_blocked_execution(virtual, progress, cause, plan.blocked)
        if target == "blocked"
        else None
    )
    progress = await candidate_completion_progress(
        db,
        reference.run_id,
        current,
        progress,
        cp.owner,
        completion.finished_at,
        active=virtual.active,
        plan=plan,
        blocked_evidence=blocked,
    )
    operation = CandidateCompletionOperation(
        reference,
        response_digest(result),
        candidate_source_digest(reference.run_id, source),
        inputs_digest(inputs),
        completion_digest(completion),
        first_candidate,
    )
    return _Loaded(operation, value, source, progress)


class CandidateCompletionPlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive candidate completion database budget required")
        self.transactions = transactions
        self.signing_key, self.recipient_key = signing_key, recipient_key
        self.database_seconds = database_seconds

    async def intent(self, reference: CandidateResponseReference) -> EvaluationStateIntent:
        candidate_completion_intent(reference, bytes(32))
        # Reads remain valid after terminal projection, so duplicates find their first plan.
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            result = await _response(db, reference)
            if result is None:
                raise CheckpointConflict("Durable original candidate response missing")
            return candidate_completion_intent(reference, response_digest(result))

    async def candidate(
        self, reference: CandidateResponseReference, *, candidate_id: str | None = None
    ) -> FrozenPlan:
        candidate_completion_intent(reference, bytes(32))
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            loaded = await _load(db, reference, candidate_id=candidate_id, lock=False)
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
        winner = freeze_evaluation_state_plan(facts, str(uuid4()), first)
        read_candidate_completion_plan(operation.intent, winner)
        return winner

    async def verify(self, reference: CandidateResponseReference, winner: FrozenPlan) -> None:
        intent = await self.intent(reference)
        facts, expected = read_candidate_completion_plan(intent, winner)
        if expected.reference != reference:
            raise MessageConflict("Original candidate response reference changed")
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            loaded = await _load(db, reference, candidate_id=expected.candidate_id, lock=False)
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
                raise MessageConflict("First candidate completion source or sequence changed")


class CandidateCompletionWrite:
    """Original terminal acceptor stages business; outer root commits archive and witness."""

    def __init__(self) -> None:
        self.state: CheckpointState | None = None

    async def __call__(self, db: AsyncSession, facts: EvaluationStateFacts) -> None:
        expected = CandidateCompletionOperation.decode(facts.business_context)
        if expected.intent != facts.intent:
            raise MessageConflict("Original candidate completion operation differs")
        loaded = await _load(db, expected.reference, candidate_id=expected.candidate_id, lock=True)
        if loaded.operation != expected or loaded.source.version != facts.source_version:
            raise MessageConflict("Original candidate completion source or frozen evidence changed")
        value, ref = loaded.value.completion, expected.reference
        routes = AssetSnapshotReader(db, route_assets, RouteAsset)
        if isinstance(value, GenerationCompletion):
            self.state = await complete_evaluated_generation(
                db,
                ref.run_id,
                loaded.source.version,
                ref.organization_id,
                ref.checkpoint.owner,
                value,
                routes,
                AssetSnapshotReader(db, schema_assets, SchemaAsset),
                candidate_id=expected.candidate_id,
                claim=loaded.source.claim,
            )
        else:
            self.state = await complete_semantic(
                db,
                ref.run_id,
                loaded.source.version,
                ref.organization_id,
                ref.checkpoint.owner,
                value,
                routes,
                claim=loaded.source.claim,
            )
        if self.state.version != loaded.source.version + 1 or self.state.checkpoint is not None:
            raise MessageConflict(
                "Original candidate terminal result differs from first projection"
            )
