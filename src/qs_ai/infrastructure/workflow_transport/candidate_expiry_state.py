"""Typed candidate ownership expiry; no fabricated response, lease or model permission."""

import asyncio
import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transport import RouteResolver
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import route_assets, schema_assets
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.completion_records import generation_completions
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
    read_candidate_recovery_source,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import (
    decode,
    encode,
    save_checkpoint,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import complete_generation
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_recovery import interrupted_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import complete_semantic
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    active_claims,
    release_claim,
)
from qs_ai.infrastructure.workflow_transport.candidate_completion_state import (
    project_candidate_terminal,
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
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import worker_source_digest

_MAGIC = b"qs-ai-candidate-expiry/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate candidate expiry fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class CandidateExpiryRequest:
    organization_id: int
    claim: SlotClaim = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.organization_id) is not int
            or not 0 < self.organization_id < 2**63
            or not isinstance(self.claim, SlotClaim)
            or not isinstance(self.claim.checkpoint, ExecutionCheckpoint)
            or not isinstance(self.claim.run_id, UUID)
            or not self.claim.run_id.int
            or type(self.claim.version) is not int
            or not 0 < self.claim.version < 2**63 - 1
        ):
            raise ValueError("Exact original candidate expiry ownership required")
        if decode(encode(self.claim.checkpoint)) != self.claim.checkpoint:
            raise ValueError("Canonical original candidate checkpoint required")

    def fields(self, *, expiry: bool) -> dict[str, Any]:
        cp = encode(self.claim.checkpoint)
        assert cp is not None
        if not expiry:
            cp.pop("lease_expires_at")
        return dict(
            run_id=str(self.claim.run_id),
            organization_id=self.organization_id,
            version=self.claim.version,
            checkpoint=cp,
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.organization_id),
            str(self.claim.run_id),
            f"candidate-expiry/{self.claim.version}/"
            + hashlib.sha256(self.claim.checkpoint.invocation_id.encode()).hexdigest(),
            self.claim.version,
            hashlib.sha256(
                b"qs-ai-candidate-expiry-input/v1\0" + _json(self.fields(expiry=False))
            ).digest(),
        )

    def encode(self, source: bytes) -> bytes:
        if type(source) is not bytes or len(source) != 32:
            raise ValueError("Exact original candidate expiry source required")
        raw = _MAGIC + _json(dict(request=self.fields(expiry=True), source=source.hex()))
        if len(raw) > 8192:
            raise ValueError("Candidate expiry facts exceed bounds")
        return raw

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["CandidateExpiryRequest", bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Bounded original candidate expiry facts required")
        try:
            obj = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(obj, dict) or set(obj) != {"request", "source"}:
                raise ValueError
            v = obj["request"]
            if not isinstance(v, dict) or set(v) != {
                "run_id",
                "organization_id",
                "version",
                "checkpoint",
            }:
                raise ValueError
            cp = decode(v["checkpoint"])
            if cp is None:
                raise ValueError
            first = cls(v["organization_id"], SlotClaim(UUID(v["run_id"]), v["version"], cp))
            source = bytes.fromhex(obj["source"])
            if (
                first.intent != facts.intent
                or first.encode(source) != raw
                or facts.source_version is None
                or facts.at < cp.lease_expires_at
            ):
                raise ValueError
            return first, source
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original candidate expiry facts") from None


class DurableCandidateResponseAvailable(CheckpointConflict):
    """The immutable first provider result must finalize rather than become interrupted."""


@dataclass(frozen=True)
class CandidateExpiryProjection:
    version: int
    claim: SlotClaim = field(repr=False)
    run: dict = field(repr=False)
    progress: dict = field(repr=False)
    digest: bytes = field(repr=False)
    completion: GenerationCompletion | SemanticCompletion | None = field(repr=False)


async def project_candidate_expiry(
    db: AsyncSession,
    request: CandidateExpiryRequest,
    at: datetime,
    *,
    lock: bool,
) -> CandidateExpiryProjection:
    source = await read_candidate_recovery_source(
        db,
        request.organization_id,
        request.claim,
        at,
        lock=lock,
    )
    if source is None:
        raise CheckpointConflict("Original candidate already completed")
    if source.version >= 2**63 - 1:
        raise CheckpointConflict("Candidate coordinator version exhausted")
    if source.evidence is not None:
        raise DurableCandidateResponseAvailable(
            "Original first candidate response requires finalization"
        )
    current, run = source.claim, source.run
    active = await active_claims(db, current.run_id, lock=lock)
    completion: GenerationCompletion | SemanticCompletion | None = None
    if current.checkpoint.phase == "prepared":
        progress = run["progress_json"]
    else:
        fingerprint = ""
        if current.checkpoint.kind == "semantic":
            query = generation_completions.select().where(
                generation_completions.c.run_id == str(current.run_id),
                generation_completions.c.candidate_id == current.checkpoint.candidate_id,
            )
            row = (await db.execute(query.with_for_update() if lock else query)).mappings().one()
            fingerprint = decode_completion(row).normalized_fingerprint
        completion = interrupted_completion(
            current.checkpoint, at, candidate_fingerprint=fingerprint
        )
        progress = await project_candidate_terminal(
            db,
            current.run_id,
            request.organization_id,
            run,
            current,
            active,
            completion,
            lock=lock,
        )
    claims = []
    for claim in active:
        cp = encode(claim.checkpoint)
        assert cp is not None
        cp.pop("lease_expires_at")
        claims.append([claim.version, cp])
    digest = hashlib.sha256(
        b"qs-ai-candidate-expiry-source/v1\0"
        + worker_source_digest(
            dict(run_id=str(current.run_id), version=source.version, checkpoint_json=None),
            run,
        )
        + _json(dict(claim=request.fields(expiry=True), active=claims, target=progress))
    ).digest()
    return CandidateExpiryProjection(source.version, current, run, progress, digest, completion)


async def apply_candidate_expiry(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    request, digest = CandidateExpiryRequest.from_facts(facts)
    source = await project_candidate_expiry(db, request, facts.at, lock=True)
    if source.version != facts.source_version or source.digest != digest:
        raise MessageConflict("Original candidate expiry source differs from first plan")
    current = source.claim
    if source.completion is None:
        await release_claim(db, current)
        await save_checkpoint(
            db, CheckpointState(current.run_id, source.version + 1, None), source.version
        )
    elif isinstance(source.completion, GenerationCompletion):
        await complete_generation(
            db,
            current.run_id,
            source.version,
            request.organization_id,
            current.checkpoint.owner,
            source.completion,
            AssetSnapshotReader(db, route_assets, RouteAsset),
            AssetSnapshotReader(db, schema_assets, SchemaAsset),
            claim=current,
        )
    else:
        await complete_semantic(
            db,
            current.run_id,
            source.version,
            request.organization_id,
            current.checkpoint.owner,
            source.completion,
            AssetSnapshotReader(db, route_assets, RouteAsset),
            claim=current,
        )


def read_candidate_expiry_plan(
    request: CandidateExpiryRequest,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, CandidateExpiryRequest, FrozenArchive]:
    facts, archive = read_evaluation_state_plan(
        request.intent, winner, route_resolver=route_resolver
    )
    first, _ = CandidateExpiryRequest.from_facts(facts)
    visible = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        first.intent != request.intent
        or facts.source_version is None
        or visible.version != facts.source_version + 1
        or visible.status not in ("collecting", "blocked")
    ):
        raise MessageConflict("Exact first candidate expiry projection required")
    return facts, first, archive


class CandidateExpiryPlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive candidate expiry database budget required")
        self.transactions, self.database_seconds = transactions, database_seconds
        self.signing_key, self.recipient_key = signing_key, recipient_key

    async def candidate(self, request: CandidateExpiryRequest, at: datetime) -> FrozenPlan:
        EvaluationStateFacts(request.intent, at, 0, 0)
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            source = await project_candidate_expiry(db, request, at, lock=False)
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(request.claim.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            facts = EvaluationStateFacts(
                request.intent,
                at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                request.encode(source.digest),
                source_version=source.version,
            )
            visible = evaluation_state(
                str(request.claim.run_id),
                {
                    **source.run,
                    "version": source.version + 1,
                    "progress_json": source.progress,
                },
                facts.sequence,
            )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(request.claim.run_id, f"state:{visible.version}")),
            str(request.claim.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(request.organization_id),
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(
            facts, str(uuid4()), first, route_resolver=self._route_resolver
        )
        read_candidate_expiry_plan(request, winner, route_resolver=self._route_resolver)
        return winner
