"""Readonly serial expiry projection; interrupted outcomes are not provider receipts."""

import asyncio
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import route_assets, schema_assets
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.completion_records import generation_completions
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_completion_progress import (
    generation_completion_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import read_generation_acceptance
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_recovery import (
    interrupted_completion,
    read_expired_recovery_source,
    recover_expired,
    recovery_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import (
    project_semantic_progress,
    read_semantic_acceptance,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_response_receipts,
)
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
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

_MAGIC = b"qs-ai-expired-recovery/v1\0"


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate expiry fact")
        result[key] = value
    return result


@dataclass(frozen=True)
class ExpiredRecoveryRequest:
    run_id: UUID
    organization_id: int
    version: int
    invocation_id: str
    observed_expiry: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, UUID)
            or not self.run_id.int
            or type(self.organization_id) is not int
            or not 0 < self.organization_id < 2**63
            or type(self.version) is not int
            or not 0 < self.version < 2**63 - 1
            or not isinstance(self.invocation_id, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.invocation_id) is None
            or not isinstance(self.observed_expiry, datetime)
            or self.observed_expiry.utcoffset() is None
        ):
            raise ValueError("Exact bounded original expired serial identity required")

    def fields(self) -> dict[str, Any]:
        return dict(
            run_id=str(self.run_id),
            organization_id=self.organization_id,
            version=self.version,
            invocation_id=self.invocation_id,
            observed_expiry=self.observed_expiry.isoformat(timespec="microseconds"),
        )

    @property
    def intent(self) -> EvaluationStateIntent:
        return EvaluationStateIntent(
            str(self.organization_id),
            str(self.run_id),
            f"expired/{self.version}/" + hashlib.sha256(self.invocation_id.encode()).hexdigest(),
            self.version,
            hashlib.sha256(b"qs-ai-expired-recovery-input/v1\0" + _json(self.fields())).digest(),
        )

    def encode(self, actor: str, source: bytes) -> bytes:
        if (
            type(actor) is not str
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", actor) is None
            or type(source) is not bytes
            or len(source) != 32
        ):
            raise ValueError("Exact bounded original expiry evidence required")
        return _MAGIC + _json(dict(request=self.fields(), actor=actor, source=source.hex()))

    @classmethod
    def from_facts(cls, facts: EvaluationStateFacts) -> tuple["ExpiredRecoveryRequest", str, bytes]:
        raw = facts.business_context
        if type(raw) is not bytes or len(raw) > 8192 or not raw.startswith(_MAGIC):
            raise ValueError("Bounded original expiry facts required")
        try:
            obj = json.loads(raw[len(_MAGIC) :], object_pairs_hook=_unique)
            if not isinstance(obj, dict) or set(obj) != {"request", "actor", "source"}:
                raise ValueError
            value = obj["request"]
            if not isinstance(value, dict) or set(value) != {
                "run_id",
                "organization_id",
                "version",
                "invocation_id",
                "observed_expiry",
            }:
                raise ValueError
            first = cls(
                UUID(value["run_id"]),
                value["organization_id"],
                value["version"],
                value["invocation_id"],
                datetime.fromisoformat(value["observed_expiry"]),
            )
            source = bytes.fromhex(obj["source"])
            if (
                first.intent != facts.intent
                or first.encode(obj["actor"], source) != raw
                or facts.source_version is not None
                or facts.at < first.observed_expiry
            ):
                raise ValueError
            return first, obj["actor"], source
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
            raise ValueError("Invalid frozen original expiry facts") from None


class DurableSerialResponseAvailable(CheckpointConflict):
    """Original durable response must finalize; expiry cannot overwrite it with Unknown."""


@dataclass(frozen=True)
class ExpiredRecoveryProjection:
    run: dict = field(repr=False)
    progress: dict = field(repr=False)
    digest: bytes = field(repr=False)


async def project_expired_recovery(
    db: AsyncSession, request: ExpiredRecoveryRequest, actor: str, at: datetime, *, lock: bool
) -> ExpiredRecoveryProjection:
    source = await read_expired_recovery_source(
        db,
        request.run_id,
        request.version,
        request.organization_id,
        request.invocation_id,
        request.observed_expiry,
        at,
        actor,
        lock=lock,
    )
    cp, run = source.checkpoint, source.run
    if run["execution_mode"] != "serial_v1":
        raise CheckpointConflict("Expired serial recovery cannot impersonate candidate ownership")
    routes = AssetSnapshotReader(db, route_assets, RouteAsset)
    schemas = AssetSnapshotReader(db, schema_assets, SchemaAsset)
    if cp.phase == "prepared":
        ledger = select(evaluation_dispatches.c.invocation_id).where(
            evaluation_dispatches.c.run_id == str(request.run_id),
            evaluation_dispatches.c.execution_id == cp.execution_id,
        )
        if (await db.execute(ledger.with_for_update() if lock else ledger)).first() is not None:
            raise ValueError("Prepared checkpoint has dispatch evidence")
        progress = run["progress_json"]
    else:
        receipt = select(evaluation_response_receipts.c.invocation_id).where(
            evaluation_response_receipts.c.run_id == str(request.run_id),
            evaluation_response_receipts.c.invocation_id == cp.invocation_id,
        )
        if (await db.execute(receipt.with_for_update() if lock else receipt)).first() is not None:
            raise DurableSerialResponseAvailable("Original durable response requires finalization")
        if cp.kind == "generation":
            completion = interrupted_completion(cp, at)
            assert isinstance(completion, GenerationCompletion)
            accepted = await read_generation_acceptance(
                db,
                request.run_id,
                run,
                cp,
                cp.owner,
                completion,
                routes,
                schemas,
                None,
                lock=lock,
            )
            progress = generation_completion_progress(
                run["progress_json"],
                completion,
                accepted.policy,
                cp.owner,
                candidate_claim=False,
            )
        else:
            query = generation_completions.select().where(
                generation_completions.c.run_id == str(request.run_id),
                generation_completions.c.candidate_id == cp.candidate_id,
            )
            generated = (
                (await db.execute(query.with_for_update() if lock else query)).mappings().one()
            )
            completion = interrupted_completion(
                cp,
                at,
                candidate_fingerprint=decode_completion(generated).normalized_fingerprint,
            )
            assert isinstance(completion, SemanticCompletion)
            semantic = await read_semantic_acceptance(
                db,
                request.run_id,
                request.organization_id,
                cp.owner,
                completion,
                run,
                cp,
                routes,
                lock=lock,
            )
            progress = await project_semantic_progress(
                db,
                request.run_id,
                completion,
                cp.owner,
                run,
                semantic,
                candidate_claim=False,
                lock=lock,
                before_insert=True,
            )
    progress = recovery_progress(progress, cp, request.version, request.observed_expiry, at, actor)
    digest = hashlib.sha256(
        b"qs-ai-expired-recovery-source/v1\0"
        + source_snapshot_digest(
            dict(run_id=str(request.run_id), version=request.version, checkpoint_json=encode(cp)),
            run,
        )
        + _json(progress)
    ).digest()
    return ExpiredRecoveryProjection(run, progress, digest)


async def apply_expired_recovery(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    request, actor, digest = ExpiredRecoveryRequest.from_facts(facts)
    current = await project_expired_recovery(db, request, actor, facts.at, lock=True)
    if current.digest != digest:
        raise MessageConflict("Original expired recovery source differs from frozen plan")
    await recover_expired(
        db,
        request.run_id,
        request.version,
        request.organization_id,
        request.invocation_id,
        request.observed_expiry,
        facts.at,
        actor,
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
    )


def read_expired_recovery_plan(
    request: ExpiredRecoveryRequest,
    winner: FrozenPlan,
) -> tuple[EvaluationStateFacts, str, FrozenArchive]:
    facts, archive = read_evaluation_state_plan(request.intent, winner)
    first, actor, _ = ExpiredRecoveryRequest.from_facts(facts)
    visible = pb.MessagingBody.FromString(archive.body).evaluation_state
    if (
        first != request
        or visible.version != request.version + 1
        or visible.status not in ("collecting", "blocked")
    ):
        raise MessageConflict("Exact first expired recovery projection required")
    return facts, actor, archive


class ExpiredRecoveryPlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive expired recovery database budget required")
        self.transactions, self.database_seconds = transactions, database_seconds
        self.signing_key, self.recipient_key = signing_key, recipient_key

    async def candidate(
        self, request: ExpiredRecoveryRequest, actor: str, at: datetime
    ) -> FrozenPlan:
        EvaluationStateFacts(request.intent, at, 0, 0)
        request.encode(actor, bytes(32))
        if at < request.observed_expiry:
            raise CheckpointConflict("Exact expired checkpoint required")
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            projection = await project_expired_recovery(db, request, actor, at, lock=False)
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(request.run_id)
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
                request.encode(actor, projection.digest),
            )
            visible = evaluation_state(
                str(request.run_id),
                {
                    **projection.run,
                    "version": request.version + 1,
                    "progress_json": projection.progress,
                },
                facts.sequence,
            )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(request.run_id, f"state:{visible.version}")),
            str(request.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(request.organization_id),
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(facts, str(uuid4()), first)
        read_expired_recovery_plan(request, winner)
        return winner
