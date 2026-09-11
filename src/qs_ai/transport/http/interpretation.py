from typing import Annotated
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Header, Request
from pydantic import BaseModel, ConfigDict, Field

from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand, StartCommand
from qs_ai.application.interpretation.ports import IdentityVerifier, Receipt
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.domain.interpretation.model import Actor

router = APIRouter(prefix="/v1/interpretation-sessions", route_class=DishkaRoute)
Key = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)]
ExternalID = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,19}$")]


class CreateBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    testee_id: ExternalID
    assessment_ids: list[ExternalID] = Field(min_length=1, max_length=10)
    goal: str = Field(min_length=1, max_length=2000)


class VersionBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_version: int = Field(ge=1)


class AnswerBody(VersionBody):
    question_id: str = Field(min_length=36, max_length=36)
    answer: str | None = Field(default=None, max_length=4000)
    skip: bool = False


class QuestionView(BaseModel):
    id: str
    text: str
    can_skip: bool


class SessionResponse(BaseModel):
    id: str
    testee_id: str
    assessment_ids: list[str]
    goal: str
    status: str
    version: int
    failure_code: str | None
    question: QuestionView | None


async def actor(request: Request, identity: IdentityVerifier) -> Actor:
    return await identity.authenticate(request.headers.get("authorization"))


@router.post("", status_code=201)
async def create_session(
    body: CreateBody,
    request: Request,
    idempotency_key: Key,
    service: FromDishka[InterpretationService],
    identity: FromDishka[IdentityVerifier],
) -> Receipt:
    return await service.create(
        await actor(request, identity),
        body.testee_id,
        tuple(body.assessment_ids),
        body.goal,
        idempotency_key,
    )


@router.get("/{session_id}")
async def get_session(
    session_id: UUID,
    request: Request,
    service: FromDishka[InterpretationService],
    identity: FromDishka[IdentityVerifier],
) -> SessionResponse:
    result = await service.get(await actor(request, identity), str(session_id))
    session = result.session
    question = result.question
    return SessionResponse(
        id=session.id,
        testee_id=session.testee_id,
        assessment_ids=list(session.assessment_ids),
        goal=session.goal,
        status=session.status,
        version=session.version,
        failure_code=session.failure_code,
        question=QuestionView(id=question.id, text=question.text, can_skip=question.can_skip)
        if question
        else None,
    )


@router.post("/{session_id}/runs", status_code=202)
async def start_session(
    session_id: UUID,
    body: VersionBody,
    request: Request,
    idempotency_key: Key,
    service: FromDishka[InterpretationService],
    identity: FromDishka[IdentityVerifier],
) -> Receipt:
    return await service.start(
        await actor(request, identity),
        str(session_id),
        StartCommand(body.expected_version),
        idempotency_key,
    )


@router.post("/{session_id}/answers", status_code=202)
async def answer_session(
    session_id: UUID,
    body: AnswerBody,
    request: Request,
    idempotency_key: Key,
    service: FromDishka[InterpretationService],
    identity: FromDishka[IdentityVerifier],
) -> Receipt:
    return await service.answer(
        await actor(request, identity),
        str(session_id),
        AnswerCommand(body.expected_version, body.question_id, body.answer, body.skip),
        idempotency_key,
    )


@router.post("/{session_id}/cancel")
async def cancel_session(
    session_id: UUID,
    body: VersionBody,
    request: Request,
    idempotency_key: Key,
    service: FromDishka[InterpretationService],
    identity: FromDishka[IdentityVerifier],
) -> Receipt:
    return await service.cancel(
        await actor(request, identity),
        str(session_id),
        CancelCommand(body.expected_version),
        idempotency_key,
    )
