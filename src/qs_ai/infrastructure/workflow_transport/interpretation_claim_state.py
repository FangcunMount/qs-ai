"""Freeze one original worker candidate; planning never leases or authorizes a model."""

import asyncio
import hashlib
import json
import math
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.domain.interpretation.model import Session, Status
from qs_ai.infrastructure.persistence.mysql.execution import ClaimStage, MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.interpretation import session_from
from qs_ai.infrastructure.persistence.mysql.schema import execution_leases, jobs, sessions
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    InterpretationStateFacts,
    InterpretationStateIntent,
    freeze_interpretation_state_plan,
    read_interpretation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare, valid_id
from qs_ai.infrastructure.workflow_transport.state_projection import interpretation_state
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


@dataclass(frozen=True)
class InterpretationClaimSource:
    session: Session = field(repr=False)
    request_id: str
    job_id: str
    attempt: int
    fence: int
    source_hash: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            any(
                not isinstance(v, str) or not valid_id(v)
                for v in (self.session.id, self.session.active_run_id, self.request_id, self.job_id)
            )
            or self.session.status not in (Status.QUEUED, Status.RUNNING)
            or type(self.attempt) is not int
            or not 0 <= self.attempt < 2**31
            or type(self.fence) is not int
            or not 0 <= self.fence < 2**63 - 1
            or type(self.source_hash) is not bytes
            or len(self.source_hash) != 32
        ):
            raise ValueError("Exact original scoped claim source required")
        object.__setattr__(self, "session", replace(self.session))

    @property
    def exhausted(self) -> bool:
        return self.attempt >= 3

    def intent(self, ttl_seconds: int) -> InterpretationStateIntent:
        if type(ttl_seconds) is not int or not 3 <= ttl_seconds <= 86400:
            raise ValueError("Bounded original claim TTL required")
        return InterpretationStateIntent(
            self.session.actor.org_id,
            self.session.id,
            self.request_id,
            f"claim/{self.job_id}/{self.attempt}/{self.fence}",
            self.session.version,
            hashlib.sha256(
                b"qs-ai-interpretation-claim-input/v1\0"
                + _json(
                    dict(
                        source_hash=self.source_hash.hex(),
                        ttl_seconds=ttl_seconds,
                        session=asdict(self.session),
                        request_id=self.request_id,
                        job_id=self.job_id,
                        attempt=self.attempt,
                        fence=self.fence,
                    )
                )
            ).digest(),
        )


async def read_claim_source(
    store: MySQLExecutionStore,
    db: AsyncSession,
    job_id: str,
    session_id: str,
    *,
    lock: bool = False,
) -> InterpretationClaimSource:
    """Advisory source or original locked recheck; never insert, commit or renew."""
    if any(not isinstance(v, str) or not valid_id(v) for v in (job_id, session_id)):
        raise ValueError("Canonical original claim candidate required")
    query = select(sessions).where(sessions.c.id == session_id)
    if lock:
        query = query.with_for_update(skip_locked=True)
    row = (await db.execute(query)).mappings().one_or_none()
    if row is None:
        raise LeaseLost("Original claim Session unavailable")
    session = session_from(row)
    query = select(
        jobs,
        (
            ((jobs.c.status == "queued") & (jobs.c.available_at <= func.utc_timestamp(6)))
            | ((jobs.c.status == "leased") & (jobs.c.lease_until <= func.utc_timestamp(6)))
        ).label("eligible"),
    ).where(jobs.c.id == job_id)
    if lock:
        query = query.with_for_update(skip_locked=True)
    job = (await db.execute(query)).mappings().one_or_none()
    if (
        job is None
        or not job["eligible"]
        or job["session_id"] != session.id
        or job["run_id"] != session.active_run_id
        or session.status not in (Status.QUEUED, Status.RUNNING)
    ):
        raise LeaseLost("Original claim candidate differs or is not due")
    query = select(
        execution_leases, (execution_leases.c.expires_at <= func.utc_timestamp(6)).label("expired")
    ).where(execution_leases.c.thread_id == session.id)
    if lock:
        query = query.with_for_update()
    lease = (await db.execute(query)).mappings().one_or_none()
    # Original exhausted admission invalidates even a still-active stale lease.
    if job["attempt"] < 3 and lease is not None and not lease["expired"]:
        raise LeaseLost("Original claim lease is still active")
    source_hash = hashlib.sha256(
        _json(
            dict(
                session=asdict(session),
                request_id=row["request_id"],
                job={
                    k: job[k]
                    for k in (
                        "id",
                        "session_id",
                        "run_id",
                        "status",
                        "attempt",
                        "fence_token",
                        "answer",
                        "skipped",
                        "question_id",
                    )
                },
                lease_fence=lease["fence"] if lease is not None else 0,
                lease_present=lease is not None,
            )
        )
    ).digest()
    return InterpretationClaimSource(
        session,
        row["request_id"],
        job_id,
        job["attempt"],
        lease["fence"] if lease is not None else 0,
        source_hash,
    )


def _projection(source: InterpretationClaimSource) -> Session:
    value = replace(source.session)
    value.running()
    if source.exhausted:
        value.block("attempts_exhausted")
    return value


def read_claim_plan(
    source: InterpretationClaimSource,
    ttl_seconds: int,
    winner: FrozenPlan,
) -> tuple[InterpretationStateFacts, FrozenArchive]:
    intent = source.intent(ttl_seconds)
    facts, archive = read_interpretation_state_plan(intent, winner)
    try:
        context = json.loads(facts.business_context)
        if (
            set(context) != {"kind", "source_hash", "event_id", "ttl_seconds"}
            or context["kind"] != "interpretation-claim/v1"
            or context["source_hash"] != source.source_hash.hex()
            or context["event_id"] != archive.envelope().message_id
            or type(context["ttl_seconds"]) is not int
            or context["ttl_seconds"] != ttl_seconds
            or facts.version_delta != (2 if source.exhausted else 1)
            or _json(context) != facts.business_context
        ):
            raise ValueError
        expected = interpretation_state(context["event_id"], source.request_id, _projection(source))
        if (
            workflow.StateEvent(**asdict(expected))
            != pb.MessagingBody.FromString(archive.body).interpretation_state
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise ValueError("Frozen original interpretation claim differs") from None
    return facts, archive


class InterpretationClaimPlanner:
    def __init__(
        self,
        store: MySQLExecutionStore,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive claim planning budget required")
        self.store, self.signing_key, self.recipient_key = store, signing_key, recipient_key
        self.database_seconds = database_seconds

    async def source(self, job_id: str, session_id: str) -> InterpretationClaimSource:
        async with asyncio.timeout(self.database_seconds):
            async with self.store.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                return await read_claim_source(self.store, db, job_id, session_id)

    async def candidate(self, source: InterpretationClaimSource, ttl_seconds: int) -> FrozenPlan:
        intent = source.intent(ttl_seconds)
        current = await self.source(source.job_id, source.session.id)
        if current != source:
            raise LeaseLost("Original claim source changed before freeze")
        # All source resources have closed before first UUID, clock and Seal.
        event_id, operation_id = str(uuid4()), str(uuid4())
        facts = InterpretationStateFacts(
            intent,
            datetime.now(UTC),
            _json(
                dict(
                    kind="interpretation-claim/v1",
                    source_hash=source.source_hash.hex(),
                    event_id=event_id,
                    ttl_seconds=ttl_seconds,
                )
            ),
            version_delta=2 if source.exhausted else 1,
        )
        value = interpretation_state(event_id, source.request_id, _projection(source))
        message = prepare(
            pb.INTERPRETATION_STATE,
            event_id,
            source.request_id,
            pb.MessagingBody(interpretation_state=workflow.StateEvent(**asdict(value))),
            organization_id=intent.organization_id,
            signing_key=self.signing_key,
            recipient_key=self.recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_interpretation_state_plan(facts, operation_id, message)
        read_claim_plan(source, ttl_seconds, winner)
        return winner


class InterpretationClaimWrite:
    """One exact original candidate; caller owns guarded root and commit proof."""

    def __init__(
        self,
        store: MySQLExecutionStore,
        source: InterpretationClaimSource,
        ttl_seconds: int,
        winner: FrozenPlan,
    ) -> None:
        self.facts, self.archive = read_claim_plan(source, ttl_seconds, winner)
        self.store, self.source, self.ttl_seconds = store, source, ttl_seconds
        self.staged: ClaimStage | None = None
        self.quota_denied = False

    async def __call__(self, db: AsyncSession, facts: InterpretationStateFacts) -> None:
        if facts != self.facts:
            raise ValueError("Original claim facts changed")
        current = await read_claim_source(
            self.store,
            db,
            self.source.job_id,
            self.source.session.id,
            lock=True,
        )
        if current != self.source:
            raise LeaseLost("Original claim source changed before original write")
        staged = await self.store.claim_in(
            db,
            self.source.job_id,
            self.source.session.id,
            self.ttl_seconds,
            state_event_id=self.archive.envelope().message_id,
            at=facts.at,
        )
        expected = "exhausted" if self.source.exhausted else "claimed"
        if staged.status != expected:
            self.quota_denied = staged.status == "deferred"
            # Capacity denial has no visible state. Never commit a prepared running
            # event for this original no-event deferral. The outer root rolls back.
            raise LeaseLost("Original claim did not produce its frozen state")
        if not self.source.exhausted and (
            staged.claim is None
            or staged.claim.fence != self.source.fence + 1
            or staged.claim.session != _projection(self.source)
        ):
            raise LeaseLost("Original staged claim differs from frozen candidate")
        self.staged = staged
