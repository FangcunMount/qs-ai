from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import Provider
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError

from qs_ai.application.interpretation.ports import (
    AccessDenied,
    DependencyUnavailable,
    NotFound,
    Unauthenticated,
)
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.transport.http.errors import application_error, invalid_request
from qs_ai.transport.http.health import router
from qs_ai.transport.http.interpretation import router as interpretation_router


def create_app(
    settings: Settings | None = None, *, providers: tuple[Provider, ...] = ()
) -> FastAPI:
    container = create_container(settings or Settings(), *providers)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await container.close()

    app = FastAPI(title="QS AI", version="0.1.0", lifespan=lifespan)
    setup_dishka(container, app)
    app.include_router(router)
    app.include_router(interpretation_router)
    for error_type in (
        AccessDenied,
        DependencyUnavailable,
        NotFound,
        Unauthenticated,
        RuleViolation,
        SQLAlchemyError,
    ):
        app.add_exception_handler(error_type, application_error)
    app.add_exception_handler(RequestValidationError, invalid_request)
    return app
