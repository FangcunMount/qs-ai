"""Read projection for the original START service, before any half message.

No business fact, reservation, Outbox row, lock, witness or root commit is
staged here. The actual host executor must repeat the original locked checks
and compare its business result with the frozen winner before committing.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.capacity import (
    DEFAULT_PARTICIPANT_CAPACITY,
    ParticipantCapacityPolicy,
)
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import (
    DEFAULT_EDITABLE_MODELS,
    EditableModelPolicy,
)
from qs_ai.application.integration.events import StateEvent
from qs_ai.application.interpretation.ports import Receipt, UnitOfWork
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceSet,
    Question,
    RuleViolation,
    Session,
    Status,
)
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    PlannedConfiguration,
    configuration_refusals,
    plan_configuration,
)
from qs_ai.infrastructure.persistence.mysql.participant_capacity import preview as preview_capacity
from qs_ai.infrastructure.persistence.mysql.schema import idempotency
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts


class ReadOnlyStartUnitOfWork:
    def __init__(
        self,
        db: AsyncSession,
        facts: StartPlanFacts,
        capacity: ParticipantCapacityPolicy = DEFAULT_PARTICIPANT_CAPACITY,
        quota_baseline: QuotaBaseline | None = None,
        models: EditableModelPolicy = DEFAULT_EDITABLE_MODELS,
    ) -> None:
        self.db, self.facts = db, facts
        self.capacity, self.quota_baseline, self.models = capacity, quota_baseline, models
        self._session: Session | None = None
        self._request_id: str | None = None
        self._result: Receipt | None = None
        self._state: StateEvent | None = None
        self._configuration: PlannedConfiguration | None = None
        self._previous = False
        self._entered = False

    @property
    def state(self) -> StateEvent | None:
        return self._state

    @property
    def configuration(self) -> PlannedConfiguration | None:
        return self._configuration

    @property
    def previous(self) -> bool:
        return self._previous

    @asynccontextmanager
    async def open(self) -> AsyncIterator[UnitOfWork]:
        if self._entered:
            raise RuntimeError("One original START read plan per unit required")
        self._entered = True
        yield self

    async def reserve(self, scope: str, key: str, request_hash: str) -> Receipt | None:
        # No insert/upsert or FOR UPDATE; the real root retains those responsibilities.
        row = (
            (
                await self.db.execute(
                    select(idempotency).where(
                        idempotency.c.scope_hash == scope,
                        idempotency.c.key == key,
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
        if row["response"] is not None:
            self._previous = True
            self._result = Receipt(**row["response"])
        return self._result

    def _original(self, session: Session) -> None:
        if session is not self._session or session.id != self.facts.session_id:
            raise RuntimeError("Original planned START session required")

    async def add(self, session: Session) -> None:
        if self._session is not None or session.id != self.facts.session_id:
            raise RuntimeError("One frozen START session required")
        self._session = session

    async def bind_request(self, session_id: str, request_id: str) -> None:
        if self._session is None or session_id != self._session.id or self._request_id is not None:
            raise RuntimeError("Original planned START request required")
        self._request_id = request_id

    async def add_evidence(self, evidence: EvidenceSet) -> None:
        if evidence.id != self.facts.evidence_id or evidence.session_id != self.facts.session_id:
            raise RuntimeError("Original planned START evidence required")

    async def bind_configuration(self, session: Session, evidence: EvidenceSet) -> None:
        self._original(session)
        with configuration_refusals():
            self._configuration = await plan_configuration(self.db, session, evidence, self.models)

    async def enqueue(
        self, session: Session, answer: str | None, skip: bool, question_id: str | None
    ) -> None:
        self._original(session)
        if answer is not None or skip or question_id is not None:
            raise RuntimeError("Only initial START read planning supported")
        if session.uses_qs_snapshot:
            await preview_capacity(
                self.db, session, self.capacity, self.facts.at, self.quota_baseline
            )

    async def reject_run(self, session: Session) -> None:
        self._original(session)
        if session.status != Status.BLOCKED or session.failure_code not in {
            "participant_daily_capacity_exceeded",
            "configuration_unavailable",
            "admission_input_invalid",
            "admission_configuration_invalid",
        }:
            raise RuntimeError("Known original START refusal required")

    async def save(self, session: Session) -> None:
        self._original(session)
        if (
            self._request_id is None
            or session.status not in (Status.QUEUED, Status.BLOCKED)
            or session.current_question_id
        ):
            raise RuntimeError("Only initial START state can be planned")
        self._state = StateEvent(
            self.facts.state_event_id,
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
            raise RuntimeError("Original planned START receipt differs")
        self._result = receipt

    async def commit(self) -> None:
        # Application port completion only; this class never commits a database.
        if self._result is None or self._state is None:
            raise RuntimeError("Complete START read projection required")

    async def get(self, session_id: str, *, lock: bool = False) -> Session:
        raise RuntimeError("Only original START read planning supported")

    async def question(self, question_id: str) -> Question:
        raise RuntimeError("Only original START read planning supported")

    async def answer(
        self, question: Question, actor: Actor, answer: str | None, skip: bool
    ) -> None:
        raise RuntimeError("Only original START read planning supported")

    async def cancel_jobs(self, session: Session) -> None:
        raise RuntimeError("Only original START read planning supported")
