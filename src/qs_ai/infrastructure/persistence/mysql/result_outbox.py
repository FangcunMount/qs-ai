from dataclasses import asdict
from uuid import uuid4

from sqlalchemy import func, select, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.integration.events import StateEvent
from qs_ai.domain.interpretation.model import Actor, Session
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import (
    external_requests,
    questions,
    result_outbox,
)


async def stage_state(db: AsyncSession, session: Session) -> None:
    request_id = await db.scalar(
        select(external_requests.c.request_id).where(external_requests.c.session_id == session.id)
    )
    if request_id is None:
        return
    question = None
    if session.current_question_id:
        question = (
            (
                await db.execute(
                    select(questions).where(questions.c.id == session.current_question_id)
                )
            )
            .mappings()
            .one()
        )
    event = StateEvent(
        str(uuid4()),
        request_id,
        session.id,
        session.actor,
        session.testee_id,
        session.version,
        session.status,
        question_id=question["id"] if question else "",
        question=question["text"] if question else "",
        can_skip=question["can_skip"] if question else False,
        failure_code=session.failure_code or "",
    )
    statement = insert(result_outbox).values(
        event_id=event.event_id,
        session_id=session.id,
        version=session.version,
        payload=asdict(event),
    )
    # Evidence freezing can save the same user-visible state/version again.
    await db.execute(statement.on_duplicate_key_update(event_id=result_outbox.c.event_id))


class MySQLResultOutbox:
    def __init__(self, transactions: Transactions, max_retry_seconds: int = 60) -> None:
        self.max_retry_seconds = max_retry_seconds
        self.transactions = transactions

    async def pending(self, limit: int) -> list[StateEvent]:
        if not 1 <= limit <= 100:
            raise ValueError("Batch limit must be 1..100")
        async with self.transactions.open() as db:
            rows = (
                await db.execute(
                    select(result_outbox.c.payload)
                    .where(
                        result_outbox.c.delivered.is_(False),
                        result_outbox.c.available_at <= func.utc_timestamp(6),
                    )
                    .order_by(result_outbox.c.available_at, result_outbox.c.event_id)
                    .limit(limit)
                )
            ).scalars()
            return [StateEvent(**{**row, "actor": Actor(**row["actor"])}) for row in rows]

    async def delivered(self, event_id: str) -> None:
        async with self.transactions.open() as db:
            await db.execute(
                update(result_outbox)
                .where(result_outbox.c.event_id == event_id)
                .values(delivered=True)
            )
            await db.commit()

    async def retry(self, event_id: str) -> None:
        from sqlalchemy import literal_column

        async with self.transactions.open() as db:
            await db.execute(
                update(result_outbox)
                .where(result_outbox.c.event_id == event_id, result_outbox.c.delivered.is_(False))
                .values(
                    attempts=result_outbox.c.attempts + 1,
                    available_at=func.timestampadd(
                        literal_column("SECOND"),
                        func.least(
                            self.max_retry_seconds,
                            func.pow(2, func.least(result_outbox.c.attempts, 17)),
                        ),
                        func.utc_timestamp(6),
                    ),
                )
            )
            await db.commit()
