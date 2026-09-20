"""Unary RPC diagnostics. Metadata is correlation only, never authorization."""

import logging
import re
from time import monotonic
from typing import Any
from uuid import uuid4

import grpc
from grpc import aio

from qs_ai.application.operations.diagnostics import context, emit


def correlation(value: object) -> str:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9._-]{1,128}", value):
        return value
    return str(uuid4())


class Diagnostics(aio.ServerInterceptor):
    async def intercept_service(self, continuation: Any, details: Any) -> Any:
        handler = await continuation(details)
        if handler is None or handler.unary_unary is None:
            return handler
        metadata = dict(details.invocation_metadata or ())
        correlation_id = correlation(metadata.get("x-correlation-id", metadata.get("x-request-id")))

        async def call(request: Any, rpc_context: Any) -> Any:
            fields = {
                key: getattr(request, key, "")
                for key in ("request_id", "command_id", "session_id", "run_id")
            }
            with context(correlation_id=correlation_id, **fields):
                start = monotonic()
                status = grpc.StatusCode.OK
                result = None
                try:
                    result = await handler.unary_unary(request, rpc_context)
                    return result
                except BaseException:
                    status = rpc_context.code() or grpc.StatusCode.UNKNOWN
                    raise
                finally:
                    status = rpc_context.code() or status
                    emit(
                        "rpc.completed",
                        "grpc",
                        stage=details.method.lstrip("/"),
                        status=status.name,
                        level=logging.INFO if status == grpc.StatusCode.OK else logging.WARNING,
                        duration_ms=(monotonic() - start) * 1000,
                        session_id=getattr(result, "session_id", fields["session_id"]),
                        run_id=getattr(result, "run_id", fields["run_id"]),
                    )

        return grpc.unary_unary_rpc_method_handler(
            call,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
