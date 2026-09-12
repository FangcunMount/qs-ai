from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Any
from uuid import uuid4

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.ports import NotFound, Receipt, UnitOfWork
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceSet,
    Question,
    RuleViolation,
    Session,
    Status,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import bind_configuration
from qs_ai.infrastructure.persistence.mysql.result_outbox import stage_state
from qs_ai.infrastructure.persistence.mysql.schema import (
    evidence_sets,
    external_requests,
    idempotency,
    jobs,
    leases,
    questions,
    runs,
    sessions,
)


def session_from(row: Mapping[Any, Any]) -> Session:
    return Session(
        id=row["id"],
        actor=Actor(str(row["org_id"]), row["owner_subject_id"]),
        testee_id=str(row["testee_id"]),
        assessment_ids=tuple(row["assessment_ids"]),
        goal=row["goal"],
        status=Status(row["status"]),
        version=row["version"],
        active_run_id=row["active_run_id"],
        current_question_id=row["current_question_id"],
        evidence_set_id=row["evidence_set_id"],
        workflow_version=row["workflow_version"],
        failure_code=row["failure_code"],
    )


def session_values(session: Session) -> dict[str, Any]:
    value = asdict(session)
    value.pop("actor")
    value.update(
        org_id=int(session.actor.org_id),
        owner_subject_id=session.actor.subject_id,
        testee_id=int(session.testee_id),
        assessment_ids=list(session.assessment_ids),
    )
    return value


class MySQLUnitOfWork:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def reserve(self, scope: str, key: str, request_hash: str) -> Receipt | None:
        statement = mysql_insert(idempotency).values(
            scope_hash=scope, key=key, request_hash=request_hash, response=None
        )
        await self.db.execute(
            statement.on_duplicate_key_update(scope_hash=idempotency.c.scope_hash)
        )
        row = (
            (
                await self.db.execute(
                    select(idempotency)
                    .where(idempotency.c.scope_hash == scope, idempotency.c.key == key)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if row["request_hash"] != request_hash:
            raise RuleViolation("idempotency_conflict")
        return Receipt(**row["response"]) if row["response"] else None

    async def receipt(self, scope: str, key: str, receipt: Receipt) -> None:
        await self.db.execute(
            update(idempotency)
            .where(idempotency.c.scope_hash == scope, idempotency.c.key == key)
            .values(response=asdict(receipt))
        )

    async def get(self, session_id: str, *, lock: bool = False) -> Session:
        statement = select(sessions).where(sessions.c.id == session_id)
        if lock:
            statement = statement.with_for_update()
        row = (await self.db.execute(statement)).mappings().first()
        if row is None:
            raise NotFound
        return session_from(row)

    async def bind_configuration(self, session: Session, evidence: EvidenceSet) -> None:
        await bind_configuration(self.db, session, evidence)

    async def bind_request(self, session_id: str, request_id: str) -> None:
        await self.db.execute(
            insert(external_requests).values(session_id=session_id, request_id=request_id)
        )

    async def add(self, session: Session) -> None:
        await self.db.execute(insert(sessions).values(**session_values(session)))

    async def add_evidence(self, evidence: EvidenceSet) -> None:
        await self.db.execute(
            insert(evidence_sets).values(
                id=evidence.id,
                session_id=evidence.session_id,
                fingerprint=evidence.fingerprint,
                schema_version="evidence-v1",
                items=[asdict(item) for item in evidence.items],
            )
        )

    async def save(self, session: Session) -> None:
        await self.db.execute(
            update(sessions)
            .where(sessions.c.id == session.id)
            .values(**session_values(session), updated_at=func.utc_timestamp(6))
        )

        await stage_state(self.db, session)

    async def question(self, question_id: str) -> Question:
        row = (
            (await self.db.execute(select(questions).where(questions.c.id == question_id)))
            .mappings()
            .one()
        )
        return Question(**{key: row[key] for key in Question.__dataclass_fields__})

    async def answer(
        self, question: Question, actor: Actor, answer: str | None, skip: bool
    ) -> None:
        await self.db.execute(
            update(questions)
            .where(questions.c.id == question.id)
            .values(
                answer=answer,
                skipped=skip,
                answered_by=actor.subject_id,
                answered_at=func.utc_timestamp(6),
            )
        )

    async def enqueue(
        self, session: Session, answer: str | None, skip: bool, question_id: str | None
    ) -> None:
        await self.db.execute(
            insert(runs).values(
                id=session.active_run_id,
                session_id=session.id,
                session_version=session.version,
                status="queued",
            )
        )
        await self.db.execute(
            insert(jobs).values(
                id=str(uuid4()),
                run_id=session.active_run_id,
                session_id=session.id,
                status="queued",
                available_at=func.utc_timestamp(6),
                fence_token=0,
                attempt=0,
                answer=answer,
                skipped=skip,
                question_id=question_id,
            )
        )

    async def cancel_jobs(self, session: Session) -> None:
        await self.db.execute(
            update(jobs)
            .where(jobs.c.session_id == session.id, jobs.c.status.in_(["queued", "leased"]))
            .values(status="cancelled")
        )
        await self.db.execute(
            update(runs)
            .where(runs.c.session_id == session.id, runs.c.status.in_(["queued", "running"]))
            .values(status="cancelled")
        )
        await self.db.execute(
            update(leases)
            .where(leases.c.thread_id == session.id)
            .values(fence=leases.c.fence + 1, expires_at=func.utc_timestamp(6))
        )

    async def commit(self) -> None:
        await self.db.commit()


class MySQLUnitOfWorkFactory:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    @asynccontextmanager
    async def open(self) -> AsyncIterator[UnitOfWork]:
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            yield MySQLUnitOfWork(db)
