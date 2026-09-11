from dataclasses import asdict
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, func, insert, literal_column, or_, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.functions import Function

from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.domain.interpretation.model import EvidenceItem, EvidenceSet, Fact, Session, Status
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork, session_from
from qs_ai.infrastructure.persistence.mysql.leases import LeaseLost
from qs_ai.infrastructure.persistence.mysql.schema import (
    evidence_sets,
    jobs,
    leases,
    questions,
    runs,
    sessions,
)


def expiry(
    ttl_seconds: int,
) -> Function[Any]:  # SQL expression uses database UTC time, never local process time.
    return func.timestampadd(literal_column("SECOND"), ttl_seconds, func.utc_timestamp(6))


class MySQLExecutionStore:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def claim(self, ttl_seconds: int) -> Claim | None:
        if ttl_seconds < 3:
            raise ValueError("Lease TTL must be at least three seconds")
        eligible = or_(
            and_(jobs.c.status == "queued", jobs.c.available_at <= func.utc_timestamp(6)),
            and_(jobs.c.status == "leased", jobs.c.lease_until <= func.utc_timestamp(6)),
        )
        async with self.transactions.open() as db:
            # Consistent lock order for commands and workers: Session -> Job -> Lease.
            candidates = (
                await db.execute(
                    select(jobs.c.id, jobs.c.session_id)
                    .where(eligible)
                    .order_by(jobs.c.available_at, jobs.c.id)
                    .limit(20)
                )
            ).all()
            for candidate in candidates:
                row = (
                    (
                        await db.execute(
                            select(sessions)
                            .where(sessions.c.id == candidate.session_id)
                            .with_for_update(skip_locked=True)
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is None:
                    continue
                job = (
                    (
                        await db.execute(
                            select(jobs)
                            .where(jobs.c.id == candidate.id, eligible)
                            .with_for_update(skip_locked=True)
                        )
                    )
                    .mappings()
                    .first()
                )
                if job is None:
                    continue
                session = session_from(row)
                if session.active_run_id != job["run_id"] or session.status not in (
                    Status.QUEUED,
                    Status.RUNNING,
                ):
                    continue
                # Infinite crash loops become a visible blocked session.
                if job["attempt"] >= 3:
                    session.running()
                    session.block("attempts_exhausted")
                    await MySQLUnitOfWork(db).save(session)
                    await db.execute(
                        update(jobs).where(jobs.c.id == job["id"]).values(status="dead")
                    )
                    await db.execute(
                        update(runs).where(runs.c.id == job["run_id"]).values(status="blocked")
                    )
                    await db.execute(
                        update(leases)
                        .where(leases.c.thread_id == session.id)
                        .values(fence=leases.c.fence + 1, expires_at=func.utc_timestamp(6))
                    )
                    await db.commit()
                    return None
                statement = mysql_insert(leases).values(
                    thread_id=session.id, fence=0, expires_at=func.utc_timestamp(6)
                )
                await db.execute(statement.on_duplicate_key_update(thread_id=leases.c.thread_id))
                guard = (
                    (
                        await db.execute(
                            select(
                                leases,
                                (leases.c.expires_at <= func.utc_timestamp(6)).label("expired"),
                            )
                            .where(leases.c.thread_id == session.id)
                            .with_for_update()
                        )
                    )
                    .mappings()
                    .one()
                )
                if not guard["expired"]:
                    continue
                fence = guard["fence"] + 1
                session.running()
                await MySQLUnitOfWork(db).save(session)
                await db.execute(
                    update(leases)
                    .where(leases.c.thread_id == session.id)
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
                await db.commit()
                return Claim(
                    job["id"],
                    job["run_id"],
                    session,
                    fence,
                    job["answer"],
                    job["skipped"],
                    job["question_id"],
                )
        return None

    async def _guard(self, db: AsyncSession, claim: Claim) -> Session:
        session = await MySQLUnitOfWork(db).get(claim.session.id, lock=True)
        job = (
            (await db.execute(select(jobs).where(jobs.c.id == claim.job_id).with_for_update()))
            .mappings()
            .one()
        )
        guard = (
            (
                await db.execute(
                    select(leases, (leases.c.expires_at > func.utc_timestamp(6)).label("active"))
                    .where(leases.c.thread_id == session.id)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
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
            select(leases.c.expires_at > func.utc_timestamp(6)).where(
                leases.c.thread_id == claim.session.id, leases.c.fence == claim.fence
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
                update(leases)
                .where(leases.c.thread_id == claim.session.id)
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

    async def freeze(self, claim: Claim, evidence: EvidenceSet) -> EvidenceSet:
        async with self.transactions.open() as db:
            session = await self._guard(db, claim)
            existing = await self._evidence(db, session.id)
            if existing:
                return existing
            evidence.validate(session.testee_id, session.assessment_ids)
            if evidence.session_id != session.id:
                raise ValueError("Evidence session does not match")
            await db.execute(
                insert(evidence_sets).values(
                    id=evidence.id,
                    session_id=session.id,
                    fingerprint=evidence.fingerprint,
                    schema_version="evidence-v1",
                    items=[asdict(item) for item in evidence.items],
                )
            )
            session.evidence_set_id = evidence.id
            await MySQLUnitOfWork(db).save(session)
            await self._check_active(db, claim)
            await db.commit()
            return evidence

    async def finish(self, claim: Claim, result: WorkflowResult) -> None:
        if result.question is not None and (
            not result.question.strip()
            or not result.checkpoint_ref
            or result.failure_code is not None
        ):
            raise ValueError("Question must have a durable checkpoint and no failure")
        async with self.transactions.open() as db:
            session = await self._guard(db, claim)
            if result.question is not None:
                question_id = str(uuid4())
                seq = (
                    await db.scalar(
                        select(func.count())
                        .select_from(questions)
                        .where(questions.c.session_id == session.id)
                    )
                    or 0
                ) + 1
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
            await MySQLUnitOfWork(db).save(session)
            await db.execute(
                update(runs)
                .where(runs.c.id == claim.run_id)
                .values(status=session.status, checkpoint_ref=result.checkpoint_ref)
            )
            await db.execute(update(jobs).where(jobs.c.id == claim.job_id).values(status="done"))
            await self._check_active(db, claim)
            await db.execute(
                update(leases)
                .where(leases.c.thread_id == session.id)
                .values(expires_at=func.utc_timestamp(6))
            )
            await db.commit()
