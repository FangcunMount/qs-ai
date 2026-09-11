from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from qs_ai.application.operations.health import CheckReadiness

router = APIRouter(route_class=DishkaRoute)


@router.get("/healthz", tags=["operations"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "qs-ai"}


@router.get("/readyz", tags=["operations"])
async def ready(query: FromDishka[CheckReadiness]) -> JSONResponse:
    result = await query.execute()
    return JSONResponse(
        {"status": "ready" if result.ready else "not_ready", "database": result.database},
        200 if result.ready else 503,
    )
