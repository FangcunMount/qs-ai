from uuid import uuid4

from fastapi import Request
from fastapi.responses import JSONResponse

from qs_ai.application.interpretation.ports import (
    AccessDenied,
    DependencyUnavailable,
    NotFound,
    Unauthenticated,
)
from qs_ai.domain.interpretation.model import RuleViolation


def response(status: int, code: str, message: str, retryable: bool = False) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "code": code,
            "safe_message": message,
            "retryable": retryable,
            "trace_id": str(uuid4()),
        },
    )


async def application_error(request: Request, error: Exception) -> JSONResponse:
    if isinstance(error, Unauthenticated):
        return response(401, "unauthenticated", "请先登录。")
    if isinstance(error, AccessDenied):
        return response(403, "forbidden", "当前无权访问此资源。")
    if isinstance(error, NotFound):
        return response(404, "not_found", "会话不存在。")
    if isinstance(error, DependencyUnavailable):
        return response(503, "dependency_unavailable", "依赖服务尚未就绪。", True)
    if isinstance(error, RuleViolation):
        status = 409 if error.code.endswith("conflict") or error.code == "invalid_state" else 422
        return response(status, error.code, "请求与当前状态不匹配或参数无效。")
    return response(
        503, "persistence_unavailable", "数据服务暂时不可用，请使用原请求标识重试。", True
    )


async def invalid_request(request: Request, error: Exception) -> JSONResponse:
    return response(422, "invalid_request", "请求参数无效。")
