from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from qs_ai.application.operations.health import CheckReadiness

router = APIRouter(route_class=DishkaRoute)


@router.get("/healthz", tags=["operations"])
async def health(request: Request) -> JSONResponse:
    runtime = request.app.state.runtime
    healthy = runtime is None or runtime.healthy
    return JSONResponse(
        {"status": "ok" if healthy else "failed", "service": "qs-ai"}, 200 if healthy else 503
    )


@router.get("/readyz", tags=["operations"])
async def ready(request: Request, query: FromDishka[CheckReadiness]) -> JSONResponse:
    result = await query.execute()
    runtime = request.app.state.runtime
    ready = result.ready and (runtime is None or (runtime.ready and runtime.healthy))
    return JSONResponse(
        {"status": "ready" if ready else "not_ready", "database": result.database},
        200 if ready else 503,
    )
