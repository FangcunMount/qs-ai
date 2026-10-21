from dataclasses import asdict
from uuid import UUID, uuid4

from reliable_messaging.sqlalchemy import bind
from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.domain.interpretation.model import Session, Status
from qs_ai.infrastructure.persistence.mysql.schema import (
    artifacts,
    questions,
    result_outbox,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.state_projection import interpretation_state


async def stage_state(db: AsyncSession, session: Session, *, event_id: str | None = None) -> None:
    if event_id is not None and str(UUID(event_id)) != event_id:
        raise ValueError("Canonical frozen state identity required")
    request_id = await db.scalar(select(sessions.c.request_id).where(sessions.c.id == session.id))
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
    payload = None
    if session.status == Status.COMPLETED:
        payload = await db.scalar(
            select(artifacts.c.payload).where(artifacts.c.session_id == session.id)
        )
    event = interpretation_state(
        str(uuid4()) if event_id is None else event_id,
        request_id,
        session,
        question,
        payload,
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
