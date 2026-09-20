"""Trusted-scope quota administration; runtime configuration never contains credentials."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import QuotaStore, QuotaValues
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.configuration_status import MySQLConfigurationStatus
from qs_ai.transport.grpc.identity import require_qs_workload
from qs_ai.transport.grpc.prompt_drafts import identifier
from qs_ai.transport.grpc.solution_input import unique_object


def response(value: dict[str, Any], history: bool = False) -> pb.QuotaResponse:
    raw = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    if len(raw.encode()) > 131072:
        raise ValueError("Quota response exceeds limit")
    return pb.QuotaResponse(
        schema_version="qs-ai-quota-history/v1" if history else "qs-ai-quota/v1", data_json=raw
    )


class QuotaManagement(rpc.QuotaManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Quota version or command unavailable")
        except DraftConflict:
            await context.abort(grpc.StatusCode.ABORTED, "Quota changed; reload before continuing")
        except (ValueError, TypeError, KeyError):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid quota configuration")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; reconcile original command"
            )

    async def read(
        self, request: pb.QuotaQuery, context: aio.ServicerContext[Any, Any], kind: str
    ) -> pb.QuotaResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(QuotaStore)
                if kind == "receipt":
                    value = await store.receipt(scope, identifier(request.command_id))
                elif kind == "history":
                    value = await store.history(scope, request.before_revision)
                else:
                    value = await store.get(scope)
            return response(value, kind == "history")
        raise AssertionError("abort must raise")

    async def write(
        self, request: pb.QuotaWrite, context: aio.ServicerContext[Any, Any], rollback: bool
    ) -> pb.QuotaResponse:
        async with self.operation(context):
            if request.ByteSize() > 16384:
                raise ValueError("Command exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            raw = json.loads(request.command_json, object_pairs_hook=unique_object)
            field = "target_revision" if rollback else "values"
            if not isinstance(raw, dict) or set(raw) != {
                "command_id",
                "expected_revision",
                "reason",
                field,
            }:
                raise ValueError("Complete quota command required")
            async with self.container() as operation:
                store = await operation.get(QuotaStore)
                value = await store.apply(
                    scope,
                    identifier(raw["command_id"]),
                    raw["expected_revision"],
                    raw["reason"],
                    datetime.now(UTC),
                    values=None if rollback else QuotaValues.parse(raw["values"]),
                    target_revision=raw["target_revision"] if rollback else None,
                )
            return response(value)
        raise AssertionError("abort must raise")

    async def Get(
        self, request: pb.QuotaQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.QuotaResponse:
        return await self.read(request, context, "get")

    async def History(
        self, request: pb.QuotaQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.QuotaResponse:
        return await self.read(request, context, "history")

    async def GetReceipt(
        self, request: pb.QuotaQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.QuotaResponse:
        return await self.read(request, context, "receipt")

    async def Update(
        self, request: pb.QuotaWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.QuotaResponse:
        return await self.write(request, context, False)

    async def Rollback(
        self, request: pb.QuotaWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.QuotaResponse:
        return await self.write(request, context, True)

    async def Status(
        self, request: pb.QuotaQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.QuotaResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Configuration status query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                reader = await operation.get(MySQLConfigurationStatus)
                value = await reader.get(scope)
            return pb.QuotaResponse(
                schema_version="qs-ai-configuration-status/v1",
                data_json=json.dumps(value, separators=(",", ":")),
            )
        raise AssertionError("abort must raise")
