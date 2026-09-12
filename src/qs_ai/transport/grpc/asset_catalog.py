"""Read-only catalog through trusted QS mTLS and the existing governance switch."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import grpc
from dishka import AsyncContainer
from grpc import aio
from pydantic import TypeAdapter

from qs_ai.application.governance.asset_catalog import (
    AssetCatalog,
    AssetKind,
    CatalogDetail,
    CatalogPage,
    CatalogQuery,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.identity import require_qs_workload

PAGE = TypeAdapter(CatalogPage)
DETAIL = TypeAdapter(CatalogDetail)


class AssetCatalogService(rpc.AssetCatalogServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Asset unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid catalog query or asset")
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Asset catalog unavailable")
        raise AssertionError("abort must raise")

    async def List(
        self, request: pb.AssetCatalogQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.AssetCatalogResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Catalog query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            query = CatalogQuery(
                cast(AssetKind, request.kind), request.identity, request.limit or 20, request.cursor
            )
            async with self.container() as operation:
                catalog = await operation.get(AssetCatalog)
                value = await catalog.list(scope, query)
            raw = PAGE.dump_json(value).decode()
            if len(raw.encode()) > 131072:
                raise ValueError("Catalog page exceeds limit")
            return pb.AssetCatalogResponse(schema_version="qs-ai-asset-page/v1", payload_json=raw)
        raise AssertionError("abort must raise")

    async def Get(
        self, request: pb.AssetCatalogGetQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.AssetCatalogResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Catalog query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                catalog = await operation.get(AssetCatalog)
                value = await catalog.get(
                    scope, cast(AssetKind, request.kind), request.identity, request.version
                )
            raw = DETAIL.dump_json(value).decode()
            if len(raw.encode()) > 4194304:
                raise ValueError("Catalog detail exceeds limit")
            return pb.AssetCatalogResponse(schema_version="qs-ai-asset-detail/v1", payload_json=raw)
        raise AssertionError("abort must raise")
