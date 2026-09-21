"""QS-authorized solution editing; no direct end-user claims or model invocation."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import (
    CreateSolution,
    PrepareSolution,
    SaveSolution,
    SolutionStore,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.model_configuration import ModelCatalogConflict
from qs_ai.transport.grpc.identity import require_qs_workload
from qs_ai.transport.grpc.prompt_drafts import identifier
from qs_ai.transport.grpc.solution_input import parse_command


def response(value: dict[str, Any], kind: str = "qs-ai-solution/v1") -> pb.SolutionResponse:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    if len(raw.encode()) > 1048576:
        raise ValueError("Solution response exceeds limit")
    return pb.SolutionResponse(schema_version=kind, data_json=raw)


class SolutionManagement(rpc.SolutionManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Solution or command unavailable")
        except ModelCatalogConflict:
            await context.abort(grpc.StatusCode.ABORTED, "model_capability_changed")
        except (DraftConflict, AssetConflict):
            await context.abort(
                grpc.StatusCode.ABORTED, "Solution changed; reload before continuing"
            )
        except (ValueError, TypeError, KeyError):
            # Never log or return input snapshots or Prompt content.
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid solution configuration")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; reconcile original command"
            )

    async def read(
        self,
        request: pb.SolutionQuery,
        context: aio.ServicerContext[Any, Any],
        kind: str,
    ) -> pb.SolutionResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(SolutionStore)
                if kind == "list":
                    value, schema = await store.list(scope, request.cursor), "qs-ai-solutions/v1"
                elif kind == "models":
                    value, schema = store.capabilities(), "qs-ai-solution-models/v1"
                elif kind == "receipt":
                    value = await store.receipt(scope, identifier(request.command_id))
                    schema = "qs-ai-solution/v1"
                else:
                    value = await store.get(scope, identifier(request.solution_id))
                    schema = "qs-ai-solution/v1"
            return response(value, schema)
        raise AssertionError("abort must raise")

    async def write(
        self,
        request: pb.SolutionWrite,
        context: aio.ServicerContext[Any, Any],
        model: type[CreateSolution] | type[SaveSolution] | type[PrepareSolution],
    ) -> pb.SolutionResponse:
        async with self.operation(context):
            if request.ByteSize() > 262144:
                raise ValueError("Command exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            command = parse_command(request.command_json, model)
            async with self.container() as operation:
                store = await operation.get(SolutionStore)
                value = await store.apply(
                    scope, identifier(request.solution_id), command, datetime.now(UTC)
                )
            return response(value)
        raise AssertionError("abort must raise")

    async def List(
        self, request: pb.SolutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.read(request, context, "list")

    async def Get(
        self, request: pb.SolutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.read(request, context, "get")

    async def GetReceipt(
        self, request: pb.SolutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.read(request, context, "receipt")

    async def GetModels(
        self, request: pb.SolutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.read(request, context, "models")

    async def Create(
        self, request: pb.SolutionWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.write(request, context, CreateSolution)

    async def Save(
        self, request: pb.SolutionWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.write(request, context, SaveSolution)

    async def Prepare(
        self, request: pb.SolutionWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.SolutionResponse:
        return await self.write(request, context, PrepareSolution)
