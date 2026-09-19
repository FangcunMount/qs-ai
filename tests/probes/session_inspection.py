"""Persistence inspection for tests, not a user-facing session read API."""

from dataclasses import dataclass

from qs_ai.application.interpretation.ports import UnitOfWorkFactory
from qs_ai.domain.interpretation.model import Question, Session


@dataclass(frozen=True)
class SessionView:
    session: Session
    question: Question | None


async def read_session(uows: UnitOfWorkFactory, session_id: str) -> SessionView:
    async with uows.open() as uow:
        session = await uow.get(session_id)
        question = (
            await uow.question(session.current_question_id) if session.current_question_id else None
        )
        return SessionView(session, question)
