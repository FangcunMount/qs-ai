import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import BindingError
from sqlalchemy import and_, func, insert, literal_column, or_, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.functions import Function

from qs_ai.application.execution.artifact import ArtifactIdentity, build_artifact
from qs_ai.application.execution.capacity import (
    DEFAULT_PARTICIPANT_CAPACITY,
    ParticipantCapacityPolicy,
)
from qs_ai.application.execution.configuration import ConfigurationUnavailable
from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.execution.generation import GeneratedExplanation
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.application.interpretation.provider import ModelCall, ProviderFailure
from qs_ai.domain.interpretation.model import EvidenceItem, EvidenceSet, Fact, Session, Status
from qs_ai.infrastructure.observability.events import emit
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import validate_generation
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork, session_from
from qs_ai.infrastructure.persistence.mysql.participant_capacity import acquire as acquire_capacity
from qs_ai.infrastructure.persistence.mysql.participant_capacity import release as release_capacity
from qs_ai.infrastructure.persistence.mysql.runtime_milestones import record
from qs_ai.infrastructure.persistence.mysql.schema import (
    artifacts,
    evidence_sets,
    execution_leases,
    jobs,
    model_calls,
    participant_retries,
    questions,
    runs,
    sessions,
)


def expiry(
    ttl_seconds: int,
) -> Function[Any]:  # SQL expression uses database UTC time, never local process time.
    return func.timestampadd(literal_column("SECOND"), ttl_seconds, func.utc_timestamp(6))


def _eligible_jobs() -> ColumnElement[bool]:
    return or_(
        and_(jobs.c.status == "queued", jobs.c.available_at <= func.utc_timestamp(6)),
        and_(jobs.c.status == "leased", jobs.c.lease_until <= func.utc_timestamp(6)),
    )


_ClaimStatus = Literal["miss", "deferred", "exhausted", "claimed"]


@dataclass(frozen=True)
class ClaimStage:
    """Original admission writes only; the caller still owns commit and execution handoff."""

    status: _ClaimStatus
    claim: Claim | None = field(default=None, repr=False)


class MySQLExecutionStore:
    def __init__(
        self,
        transactions: Transactions,
        capacity: ParticipantCapacityPolicy = DEFAULT_PARTICIPANT_CAPACITY,
        quota_baseline: QuotaBaseline | None = None,
        diagnostic_retention_days: int = 30,
    ) -> None:
        self.transactions, self.capacity = transactions, capacity
        self.quota_baseline = quota_baseline
        self.diagnostic_retention_days = diagnostic_retention_days

    async def has_model_call(self, claim: Claim) -> bool:
        async with self.transactions.open() as db:
            await self._guard(db, claim)
            return (
                await db.execute(
                    select(model_calls.c.run_id).where(model_calls.c.run_id == claim.run_id)
                )
            ).first() is not None

    async def begin_model_call(self, claim: Claim, request_json: str) -> tuple[ModelCall, bool]:
        """Commit a dispatch marker before HTTP. Only its creator may send once.

        A crash before the actual send is deliberately indistinguishable from a
        lost response. Recovery returns the existing marker and must not resend.
        """
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            session = await self._guard(db, claim)
            row = (
                (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
                .mappings()
                .first()
            )
            if row is None:
                original_request = await db.scalar(
                    select(participant_retries.c.frozen_request_json).where(
                        participant_retries.c.run_id == claim.run_id,
                        participant_retries.c.session_id == session.id,
                        participant_retries.c.organization_id == int(session.actor.org_id),
                    )
                )
                if original_request is not None:
                    request_json = original_request
            if session.uses_published_snapshot:
                try:
                    await validate_generation(
                        db,
                        session,
                        await self._evidence(db, session.id),
                        row["request_json"] if row is not None else request_json,
                    )
                except ConfigurationUnavailable:
                    raise ProviderFailure("configuration_invalid") from None
            if row is not None:
                return ModelCall(
                    row["invocation_id"],
                    row["status"],
                    row["request_json"],
                    row["response_json"],
                    row["failure_code"],
                ), False
            call = ModelCall(str(uuid4()), "dispatched", request_json, None, None)
            await db.execute(
                insert(model_calls).values(
                    run_id=claim.run_id,
                    invocation_id=call.invocation_id,
                    fence_token=claim.fence,
                    status=call.status,
                    request_json=request_json,
                    created_at=func.utc_timestamp(6),
                    created_at_utc=func.utc_timestamp(6),
                )
            )
            await record(
                db,
                session.id,
                claim.run_id,
                "model_dispatched",
                "dispatch:" + call.invocation_id,
                invocation_id=call.invocation_id,
                retention_days=self.diagnostic_retention_days,
            )
            await self._check_active(db, claim)
            await db.commit()
            emit(
                "model_dispatched",
                session_id=session.id,
                run_id=claim.run_id,
                invocation_id=call.invocation_id,
            )
            return call, True

    async def record_model_response(
        self,
        claim: Claim,
        invocation_id: str,
        *,
        response_json: str | None = None,
        failure_code: str | None = None,
        result_unknown: bool = False,
    ) -> None:
        if (response_json is None) == (failure_code is None):
            raise ValueError("Exactly one response or failure is required")
        async with self.transactions.open() as db:
            await self._guard(db, claim)
            row = (
                (
                    await db.execute(
                        select(model_calls)
                        .where(model_calls.c.run_id == claim.run_id)
                        .with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            if (
                row["invocation_id"] != invocation_id
                or row["fence_token"] != claim.fence
                or row["status"] != "dispatched"
            ):
                raise LeaseLost("Model call no longer belongs to this dispatch")
            status = (
                "response_received"
                if response_json is not None
                else ("unknown" if result_unknown else "failed")
            )
            await db.execute(
                update(model_calls)
                .where(model_calls.c.run_id == claim.run_id)
                .values(status=status, response_json=response_json, failure_code=failure_code)
            )
            await record(
                db,
                claim.session.id,
                claim.run_id,
                "model_" + status,
                "response:" + invocation_id,
                invocation_id=invocation_id,
                retention_days=self.diagnostic_retention_days,
            )
            await self._check_active(db, claim)
            await db.commit()
            emit(
                "model_" + status,
                session_id=claim.session.id,
                run_id=claim.run_id,
                invocation_id=invocation_id,
            )

    async def claim_candidates(self, db: AsyncSession) -> list[tuple[str, str]]:
        """Original bounded due-job snapshot only; no lease or execution permission."""
        rows = (
            await db.execute(
                select(jobs.c.id, jobs.c.session_id)
                .where(_eligible_jobs())
                .order_by(jobs.c.available_at, jobs.c.id)
                .limit(20)
            )
        ).all()
        return [(row.id, row.session_id) for row in rows]

    async def claim(self, ttl_seconds: int) -> Claim | None:
        if ttl_seconds < 3:
            raise ValueError("Lease TTL must be at least three seconds")
        async with self.transactions.open() as db:
            for job_id, session_id in await self.claim_candidates(db):
                if db.get_transaction() is None:
                    await db.begin()
                staged = await self.claim_in(db, job_id, session_id, ttl_seconds)
                if staged.status != "miss":
                    await db.commit()
                if staged.status in ("claimed", "exhausted"):
                    return staged.claim
        return None

    async def claim_in(
        self,
        db: AsyncSession,
        job_id: str,
        session_id: str,
        ttl_seconds: int,
        *,
        state_event_id: str | None = None,
        at: datetime | None = None,
    ) -> ClaimStage:
        """Stage one original candidate; status/Claim is never root commit proof."""
        if ttl_seconds < 3:
            raise ValueError("Lease TTL must be at least three seconds")
        if state_event_id is not None and (
            not isinstance(state_event_id, str) or str(UUID(state_event_id)) != state_event_id
        ):
            raise ValueError("Canonical first claim event identity required")
        if at is not None and (not isinstance(at, datetime) or at.utcoffset() is None):
            raise ValueError("Aware first claim clock required")
        if not isinstance(db, AsyncSession) or db.in_nested_transaction():
            raise BindingError("Original interpretation claim root required")
        original = db.get_transaction()
        if original is None or not original.is_active:
            raise BindingError("Active original interpretation claim root required")
        binding = bind(db)
        await binding.validate()

        async def done(status: _ClaimStatus, claim: Claim | None = None) -> ClaimStage:
            if db.get_transaction() is not original or not original.is_active:
                raise BindingError("Original interpretation claim root was replaced")
            await binding.validate()
            return ClaimStage(status, claim)

        eligible = _eligible_jobs()
        row = (
            (
                await db.execute(
                    select(sessions)
                    .where(sessions.c.id == session_id)
                    .with_for_update(skip_locked=True)
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return await done("miss")
        job = (
            (
                await db.execute(
                    select(jobs)
                    .where(jobs.c.id == job_id, eligible)
                    .with_for_update(skip_locked=True)
                )
            )
            .mappings()
            .first()
        )
        if job is None:
            return await done("miss")
        session = session_from(row)
        if session.active_run_id != job["run_id"] or session.status not in (
            Status.QUEUED,
            Status.RUNNING,
        ):
            return await done("miss")
        # Infinite crash loops become a visible blocked session.
        if job["attempt"] >= 3:
            session.running()
            session.block("attempts_exhausted")
            await MySQLUnitOfWork(db, state_event_id=state_event_id).save(session)
            await db.execute(update(jobs).where(jobs.c.id == job["id"]).values(status="dead"))
            await db.execute(
                update(runs).where(runs.c.id == job["run_id"]).values(status="blocked")
            )
            await db.execute(
                update(execution_leases)
                .where(execution_leases.c.thread_id == session.id)
                .values(fence=execution_leases.c.fence + 1, expires_at=func.utc_timestamp(6))
            )
            if session.uses_qs_snapshot:
                await release_capacity(db, session, (datetime.now(UTC) if at is None else at))
            return await done("exhausted")
        statement = mysql_insert(execution_leases).values(
            thread_id=session.id, fence=0, expires_at=func.utc_timestamp(6)
        )
        await db.execute(statement.on_duplicate_key_update(thread_id=execution_leases.c.thread_id))
        guard = (
            (
                await db.execute(
                    select(
                        execution_leases,
                        (execution_leases.c.expires_at <= func.utc_timestamp(6)).label("expired"),
                    )
                    .where(execution_leases.c.thread_id == session.id)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if not guard["expired"]:
            return await done("miss")
        if session.uses_qs_snapshot and not await acquire_capacity(
            db,
            session,
            self.capacity,
            (datetime.now(UTC) if at is None else at),
            self.quota_baseline,
        ):
            # The owned scanner commits this deferral before its next candidate.
            await db.execute(
                update(jobs).where(jobs.c.id == job["id"]).values(available_at=expiry(1))
            )
            return await done("deferred")
        fence = guard["fence"] + 1
        await record(
            db,
            session.id,
            job["run_id"],
            "execution_claimed",
            f"claim:{job['id']}:{fence}",
            attempt=job["attempt"] + 1,
            retention_days=self.diagnostic_retention_days,
        )
        session.running()
        await MySQLUnitOfWork(db, state_event_id=state_event_id).save(session)
        await db.execute(
            update(execution_leases)
            .where(execution_leases.c.thread_id == session.id)
            .values(fence=fence, expires_at=expiry(ttl_seconds))
        )
        await db.execute(
            update(jobs)
            .where(jobs.c.id == job["id"])
            .values(
                status="leased",
                fence_token=fence,
                lease_until=expiry(ttl_seconds),
                attempt=job["attempt"] + 1,
            )
        )
        await db.execute(
            update(runs)
            .where(runs.c.id == job["run_id"])
            .values(status="running", session_version=session.version)
        )
        return await done(
            "claimed",
            Claim(
                job["id"],
                job["run_id"],
                session,
                fence,
                job["answer"],
                job["skipped"],
                job["question_id"],
            ),
        )

    async def _guard(self, db: AsyncSession, claim: Claim) -> Session:
        return await self.read_claim(db, claim, lock=True)

    async def read_claim(self, db: AsyncSession, claim: Claim, *, lock: bool = False) -> Session:
        """Read original live ownership; a readonly snapshot does not authorize a write."""
        session = await MySQLUnitOfWork(db).get(claim.session.id, lock=lock)
        job_query = select(jobs).where(jobs.c.id == claim.job_id)
        lease_query = select(
            execution_leases,
            (execution_leases.c.expires_at > func.utc_timestamp(6)).label("active"),
        ).where(execution_leases.c.thread_id == session.id)
        if lock:
            job_query, lease_query = job_query.with_for_update(), lease_query.with_for_update()
        job = (await db.execute(job_query)).mappings().one()
        guard = (await db.execute(lease_query)).mappings().one()
        if (
            session.status != Status.RUNNING
            or session.active_run_id != claim.run_id
            or session.version != claim.session.version
            or job["status"] != "leased"
            or job["fence_token"] != claim.fence
            or guard["fence"] != claim.fence
            or not guard["active"]
        ):
            raise LeaseLost("Execution lease no longer owns this session")
        return session

    @staticmethod
    async def _check_active(db: AsyncSession, claim: Claim) -> None:
        active = await db.scalar(
            select(execution_leases.c.expires_at > func.utc_timestamp(6)).where(
                execution_leases.c.thread_id == claim.session.id,
                execution_leases.c.fence == claim.fence,
            )
        )
        if not active:
            raise LeaseLost("Execution lease expired before commit")

    async def renew(self, claim: Claim, ttl_seconds: int) -> None:
        if ttl_seconds < 3:
            raise ValueError("Lease TTL must be at least three seconds")
        async with self.transactions.open() as db:
            await self._guard(db, claim)
            await db.execute(
                update(execution_leases)
                .where(execution_leases.c.thread_id == claim.session.id)
                .values(expires_at=expiry(ttl_seconds))
            )
            await db.execute(
                update(jobs)
                .where(jobs.c.id == claim.job_id)
                .values(lease_until=expiry(ttl_seconds))
            )
            await db.commit()

    @staticmethod
    async def _evidence(db: AsyncSession, session_id: str) -> EvidenceSet | None:
        row = (
            (
                await db.execute(
                    select(evidence_sets).where(evidence_sets.c.session_id == session_id)
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        items = tuple(
            EvidenceItem(**{**item, "facts": tuple(Fact(**fact) for fact in item["facts"])})
            for item in row["items"]
        )
        return EvidenceSet(row["id"], row["session_id"], row["fingerprint"], items)

    async def evidence(self, claim: Claim) -> EvidenceSet | None:
        async with self.transactions.open() as db:
            await self._guard(db, claim)
            return await self._evidence(db, claim.session.id)

    @staticmethod
    def _validate_finish(result: WorkflowResult) -> None:
        if result.artifact is not None and (result.question is not None or result.failure_code):
            raise ValueError("Artifact cannot accompany a question or failure")
        if result.question is not None and (
            not result.question.strip()
            or not result.checkpoint_ref
            or result.failure_code is not None
        ):
            raise ValueError("Question must have a durable checkpoint and no failure")

    async def finish(self, claim: Claim, result: WorkflowResult) -> None:
        self._validate_finish(result)
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await self.finish_in(db, claim, result)
            await db.commit()

    async def finish_in(
        self,
        db: AsyncSession,
        claim: Claim,
        result: WorkflowResult,
        *,
        question_id: str | None = None,
        question_sequence: int | None = None,
        state_event_id: str | None = None,
        at: datetime | None = None,
        artifact_identity: ArtifactIdentity | None = None,
    ) -> None:
        """Stage only on the caller's root; captured return is not commit proof.

        The transaction sender supplies first identities/time before entering its
        executor. The original owned-root path retains its existing defaults.
        """
        self._validate_finish(result)
        if any(
            value is not None and (not isinstance(value, str) or str(UUID(value)) != value)
            for value in (question_id, state_event_id)
        ):
            raise ValueError("Canonical frozen finish identities required")
        if question_id is not None and result.question is None:
            raise ValueError("Frozen question identity requires an original question result")
        if question_sequence is not None and (
            result.question is None
            or type(question_sequence) is not int
            or not 0 < question_sequence < 2**31
        ):
            raise ValueError("Exact frozen original question sequence required")
        if at is not None and (not isinstance(at, datetime) or at.utcoffset() is None):
            raise ValueError("Aware frozen finish clock required")
        if artifact_identity is not None and (
            not isinstance(artifact_identity, ArtifactIdentity)
            or result.artifact is None
            or (
                artifact_identity.run_id,
                artifact_identity.invocation_id,
                artifact_identity.artifact_id,
            )
            != (claim.run_id, result.artifact.invocation_id, result.artifact.id)
        ):
            raise ValueError("Artifact identity must match the original frozen finish")
        if not isinstance(db, AsyncSession) or db.in_nested_transaction():
            raise BindingError("Original root session required for interpretation finish")
        original = db.get_transaction()
        if original is None or not original.is_active:
            raise BindingError("Active original interpretation finish root required")
        guard = bind(db)
        await guard.validate()
        session = await self._guard(db, claim)
        if result.artifact is not None:
            candidate = result.artifact
            evidence = await self._evidence(db, session.id)
            call = (
                (await db.execute(select(model_calls).where(model_calls.c.run_id == claim.run_id)))
                .mappings()
                .one()
            )
            if (
                candidate.session_id != session.id
                or candidate.run_id != claim.run_id
                or evidence is None
                or candidate.evidence_set_id != evidence.id
                or candidate.evidence_fingerprint != evidence.fingerprint
                or call["status"] != "response_received"
                or candidate.invocation_id != call["invocation_id"]
                or candidate.provider_request_id != json.loads(call["response_json"])["request_id"]
                or candidate.content_fingerprint
                != "sha256:" + hashlib.sha256(candidate.content_json.encode()).hexdigest()
            ):
                raise ValueError("Artifact does not match the durable execution")
            if session.uses_published_snapshot:
                config, frozen = await validate_generation(
                    db, session, evidence, call["request_json"]
                )
                response = JSONModelCallCodec().decode_response(call["response_json"])
                expected = build_artifact(
                    replace(claim, session=session),
                    evidence,
                    GeneratedExplanation(frozen, response),
                    config.parser,
                    identity=artifact_identity,
                )
                if candidate != expected:
                    raise ValueError("Artifact differs from accepted publication and response")
            payload = asdict(candidate)
            if len(json.dumps(payload, ensure_ascii=False).encode()) > 131072:
                raise ValueError("Artifact exceeds delivery limit")
            await db.execute(
                insert(artifacts).values(
                    id=candidate.id, session_id=session.id, run_id=claim.run_id, payload=payload
                )
            )
            session.complete()
        elif result.question is not None:
            question_id = str(uuid4()) if question_id is None else question_id
            seq = (
                await db.scalar(
                    select(func.count())
                    .select_from(questions)
                    .where(questions.c.session_id == session.id)
                )
                or 0
            ) + 1
            if question_sequence is not None and question_sequence != seq:
                raise ValueError("Frozen original question sequence changed")
            await db.execute(
                insert(questions).values(
                    id=question_id,
                    session_id=session.id,
                    question_seq=seq,
                    text=result.question,
                    can_skip=result.can_skip,
                    skipped=False,
                )
            )
            session.await_answer(question_id)
        else:
            # P1 cannot manufacture a completed interpretation without an Artifact.
            session.block(result.failure_code or "model_not_connected")
        await MySQLUnitOfWork(db, state_event_id=state_event_id).save(session)
        await db.execute(
            update(runs)
            .where(runs.c.id == claim.run_id)
            .values(status=session.status, checkpoint_ref=result.checkpoint_ref)
        )
        await db.execute(update(jobs).where(jobs.c.id == claim.job_id).values(status="done"))
        await self._check_active(db, claim)
        await db.execute(
            update(execution_leases)
            .where(execution_leases.c.thread_id == session.id)
            .values(expires_at=func.utc_timestamp(6))
        )
        if session.uses_qs_snapshot:
            await release_capacity(db, session, datetime.now(UTC) if at is None else at)
        if db.get_transaction() is not original or not original.is_active:
            raise BindingError("Original interpretation finish root was replaced")
        await guard.validate()
