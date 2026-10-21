"""SELECT-only projection of original answer/cancel; no lock or business commit."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.capacity import (
    DEFAULT_PARTICIPANT_CAPACITY,
    ParticipantCapacityPolicy,
)
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.integration.events import StateEvent
from qs_ai.application.interpretation.ports import NotFound, Receipt, UnitOfWork
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceSet,
    Question,
    RuleViolation,
    Session,
    Status,
)
from qs_ai.infrastructure.persistence.mysql.interpretation import session_from
from qs_ai.infrastructure.persistence.mysql.participant_capacity import preview as preview_capacity
from qs_ai.infrastructure.persistence.mysql.schema import idempotency, questions, sessions
from qs_ai.infrastructure.workflow_transport.messaging import valid_id


class ReadOnlyChangeUnitOfWork:
    def __init__(
        self,
        db: AsyncSession,
        *,
        at: datetime,
        run_id: str,
        state_event_id: str,
        capacity: ParticipantCapacityPolicy = DEFAULT_PARTICIPANT_CAPACITY,
        quota_baseline: QuotaBaseline | None = None,
    ) -> None:
        if at.utcoffset() is None or not valid_id(run_id) or not valid_id(state_event_id):
            raise ValueError("Frozen CHANGE resources required")
        self.db, self.at, self.run_id, self.event_id = db, at, run_id, state_event_id
        self.capacity, self.quota_baseline = capacity, quota_baseline
        self._session: Session | None = None
        self._request_id: str | None = None
        self._result: Receipt | None = None
        self._state: StateEvent | None = None
        self._entered, self._opens = False, 0

    @property
    def state(self) -> StateEvent | None:
        return self._state

    @asynccontextmanager
    async def open(self) -> AsyncIterator[UnitOfWork]:
        # Original service opens once for ownership, once for mutation planning.
        if self._entered or self._opens >= 2:
            raise RuntimeError("One original CHANGE projection required")
        self._entered = True
        self._opens += 1
        try:
            yield self
        finally:
            self._entered = False

    async def get(self, session_id: str, *, lock: bool = False) -> Session:
        row = (
            (await self.db.execute(select(sessions).where(sessions.c.id == session_id)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise NotFound
        session = session_from(row)
        if lock:
            self._session, self._request_id = session, row["request_id"]
        return session

    async def reserve(self, scope: str, key: str, request_hash: str) -> Receipt | None:
        row = (
            (
                await self.db.execute(
                    select(idempotency).where(
                        idempotency.c.scope_hash == scope, idempotency.c.key == key
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        if row["request_hash"] != request_hash:
            raise RuleViolation("idempotency_conflict")
        return Receipt(**row["response"]) if row["response"] else None

    def _original(self, session: Session) -> None:
        if session is not self._session:
            raise RuntimeError("Original planned CHANGE Session required")

    async def question(self, question_id: str) -> Question:
        row = (
            (await self.db.execute(select(questions).where(questions.c.id == question_id)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise RuntimeError("Original current question missing")
        return Question(**{key: row[key] for key in Question.__dataclass_fields__})

    async def answer(
        self, question: Question, actor: Actor, answer: str | None, skip: bool
    ) -> None:
        # The original service validates ownership, optional answer, skip and CAS.
        if (
            self._session is None
            or question.id != self._session.current_question_id
            or actor != self._session.actor
        ):
            raise RuntimeError("Original planned answer target required")

    async def enqueue(
        self, session: Session, answer: str | None, skip: bool, question_id: str | None
    ) -> None:
        self._original(session)
        if session.active_run_id != self.run_id:
            raise RuntimeError("Frozen original CHANGE Run required")
        if session.uses_qs_snapshot:
            await preview_capacity(self.db, session, self.capacity, self.at, self.quota_baseline)

    async def cancel_jobs(self, session: Session) -> None:
        self._original(session)
        if session.status != Status.CANCELLED:
            raise RuntimeError("Original planned cancellation required")

    async def save(self, session: Session) -> None:
        self._original(session)
        if session.current_question_id or session.status not in (Status.CANCELLED, Status.QUEUED):
            raise RuntimeError("Only original CHANGE state can be planned")
        # Legacy sessions with no original QS request never emitted a state event.
        if self._request_id is not None:
            self._state = StateEvent(
                self.event_id,
                self._request_id,
                session.id,
                session.actor,
                session.testee_id,
                session.version,
                session.status,
                failure_code=session.failure_code or "",
            )

    async def receipt(self, scope: str, key: str, receipt: Receipt) -> None:
        session = self._session
        if session is None or receipt != Receipt(
            session.id, session.active_run_id, session.status, session.version
        ):
            raise RuntimeError("Original planned CHANGE receipt differs")
        self._result = receipt

    async def commit(self) -> None:
        if self._result is None:
            raise RuntimeError("Complete original CHANGE projection required")

    async def bind_request(self, session_id: str, request_id: str) -> None:
        raise RuntimeError("CHANGE cannot create START facts")

    async def bind_configuration(self, session: Session, evidence: EvidenceSet) -> None:
        raise RuntimeError("CHANGE cannot replace accepted configuration")

    async def add(self, session: Session) -> None:
        raise RuntimeError("CHANGE cannot create START facts")

    async def add_evidence(self, evidence: EvidenceSet) -> None:
        raise RuntimeError("CHANGE cannot create START facts")

    async def reject_run(self, session: Session) -> None:
        raise RuntimeError("CHANGE cannot create START refusal")
