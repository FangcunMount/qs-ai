"""Reject superseded execution writes before entering their business handlers."""

from typing import Any

import grpc
from grpc import aio

from qs_ai.transport.grpc.identity import require_qs_workload

MQ_EXECUTION_METHODS = frozenset(
    {
        "/qsai.workflow.v1.Commands/Start",
        "/qsai.workflow.v1.Commands/Change",
        "/qsai.workflow.v1.ParticipantManagement/Retry",
        "/qsai.workflow.v1.EvaluationManagement/Start",
        "/qsai.workflow.v1.EvaluationManagement/Cancel",
    }
)
MQ_REQUIRED = "MQ admission required; gRPC execution command not accepted"


async def reject_execution(context: Any) -> Any:
    """Compatibility refusal also applies when a servicer is mounted directly."""
    await require_qs_workload(context)
    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, MQ_REQUIRED)
    raise AssertionError("abort must raise")


class MQExecutionCutover(aio.ServerInterceptor):
    async def intercept_service(self, continuation: Any, details: Any) -> Any:
        handler = await continuation(details)
        if details.method not in MQ_EXECUTION_METHODS or handler is None:
            return handler

        async def reject(request: Any, context: Any) -> Any:
            return await reject_execution(context)

        return grpc.unary_unary_rpc_method_handler(
            reject,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
