"""Trusted QS read access; no model calls, current-pointer lookup, or editing."""

from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.governance.flow import FlowReader
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.flow import SCHEMA
from qs_ai.transport.grpc.identity import require_qs_workload
from qs_ai.transport.grpc.prompt_drafts import identifier
from qs_ai.transport.grpc.solutions import response


class FlowManagement(rpc.FlowManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    async def read(
        self, request: pb.FlowQuery, context: aio.ServicerContext[Any, Any], *, publication: bool
    ) -> pb.SolutionResponse:
        await require_qs_workload(context)
        try:
            if request.ByteSize() > 8192 or bool(request.solution_id) == bool(
                request.publication_id
            ):
                raise ValueError("Exactly one identity required")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                reader = await operation.get(FlowReader)
                value = (
                    await reader.publication(scope, identifier(request.publication_id))
                    if publication
                    else await reader.solution(scope, identifier(request.solution_id))
                )
            return response(value, SCHEMA)
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Flow unavailable")
        except (ValueError, TypeError, KeyError):
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION, "Fixed flow evidence unavailable"
            )
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Flow service unavailable")
        raise AssertionError("abort must raise")

    async def GetSolution(
        self, request: pb.FlowQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.read(request, context, publication=False)

    async def GetPublication(
        self, request: pb.FlowQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.read(request, context, publication=True)
