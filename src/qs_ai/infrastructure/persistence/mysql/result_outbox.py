import json
from dataclasses import asdict
from uuid import uuid4

from reliable_messaging.sqlalchemy import MySQLPendingOutbox, bind
from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.integration.events import StateEvent
from qs_ai.domain.interpretation.model import Actor, Session, Status
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import (
    artifacts,
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
    artifact_json = ""
    if session.status == Status.COMPLETED:
        payload = await db.scalar(
            select(artifacts.c.payload).where(artifacts.c.session_id == session.id)
        )
        if payload is None:
            raise ValueError("Completed session requires a durable artifact")
        artifact_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
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
        artifact_json=artifact_json,
    )
    statement = insert(result_outbox).values(
        event_id=event.event_id,
        session_id=session.id,
        version=session.version,
        payload=asdict(event),
        created_at=func.utc_timestamp(6),
        available_at=func.utc_timestamp(6),
    )
    # Evidence freezing can save the same user-visible state/version again.
    await bind(db).append(statement.on_duplicate_key_update(event_id=result_outbox.c.event_id))
    recorder = db.info.get("state_events")
    if recorder is not None:
        # Read the stored first event, not the newly generated UUID/body on duplicate save.
        row = (
            (
                await db.execute(
                    select(result_outbox).where(
                        result_outbox.c.session_id == session.id,
                        result_outbox.c.version == session.version,
                    )
                )
            )
            .mappings()
            .one()
        )
        await recorder.record_interpretation(db, row)


class MySQLResultOutbox:
    def __init__(self, transactions: Transactions, max_retry_seconds: int = 60) -> None:
        self.max_retry_seconds = max_retry_seconds
        self.transactions = transactions
        self.adapter = MySQLPendingOutbox(result_outbox, max_retry_seconds=max_retry_seconds)

    async def pending(self, limit: int) -> list[StateEvent]:
        if not 1 <= limit <= 100:
            raise ValueError("Batch limit must be 1..100")
        async with self.transactions.open() as db:
            rows = (
                (
                    await db.execute(
                        select(result_outbox.c.payload)
                        .where(
                            result_outbox.c.delivered.is_(False),
                            result_outbox.c.mq_owned.is_(False),
                            result_outbox.c.available_at <= func.utc_timestamp(6),
                        )
                        .order_by(result_outbox.c.available_at, result_outbox.c.event_id)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
            return [StateEvent(**{**row, "actor": Actor(**row["actor"])}) for row in rows]

    async def delivered(self, event_id: str) -> None:
        async with self.transactions.open() as db:
            await db.begin()
            await self.adapter.delivered(db, event_id)
            await db.commit()

    async def retry(self, event_id: str) -> None:
        async with self.transactions.open() as db:
            await db.begin()
            await self.adapter.retry(db, event_id)
            await db.commit()
