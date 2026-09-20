"""QS workload only, read-only runtime diagnostics with bounded responses."""

import json
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.execution.runtime import RuntimeReader, session_ids
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.identity import require_qs_workload


class RuntimeManagement(rpc.RuntimeManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    async def read(
        self, request: pb.RuntimeQuery, context: aio.ServicerContext[Any, Any], *, detail: bool
    ) -> pb.RuntimeResponse:
        await require_qs_workload(context)
        try:
            if request.ByteSize() > 8192:
                raise ValueError("Query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            ids = session_ids(list(request.session_ids))
            if detail and len(ids) != 1:
                raise ValueError("Detail requires one session")
            async with self.container() as operation:
                reader = await operation.get(RuntimeReader)
                data = (
                    await reader.detail(scope, ids[0])
                    if detail
                    else await reader.summaries(scope, ids)
                )
            encoded = json.dumps(data, ensure_ascii=False, allow_nan=False)
            if len(encoded.encode()) > 262144:
                raise ValueError("Response exceeds limit")
            return pb.RuntimeResponse(
                schema_version="qs-ai-runtime-detail/v1" if detail else "qs-ai-runtime-batch/v1",
                data_json=encoded,
            )
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Execution unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid runtime query")
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Runtime evidence unavailable")
        raise AssertionError("abort must raise")

    async def BatchGet(
        self, request: pb.RuntimeQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.RuntimeResponse:
        return await self.read(request, context, detail=False)

    async def Get(
        self, request: pb.RuntimeQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.RuntimeResponse:
        return await self.read(request, context, detail=True)
