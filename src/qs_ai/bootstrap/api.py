from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import AsyncContainer, Provider
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.transport.http.health import router
from qs_ai.transport.http.metrics import router as metrics_router


def create_app(
    settings: Settings | None = None,
    *,
    providers: tuple[Provider, ...] = (),
    container: AsyncContainer | None = None,
    runtime: object | None = None,
) -> FastAPI:
    owns_container = container is None
    container = container or create_container(settings or Settings(), *providers)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_container:
                await container.close()

    app = FastAPI(title="QS AI", version="0.1.0", lifespan=lifespan)
    app.state.runtime = runtime
    setup_dishka(container, app)
    app.include_router(router)
    app.include_router(metrics_router)
    return app
