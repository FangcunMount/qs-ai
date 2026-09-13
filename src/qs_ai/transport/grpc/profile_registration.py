"""Trusted QS registration of immutable Profile versions and scoped command receipts."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio
from pydantic import TypeAdapter

from qs_ai.application.governance.profile_lifecycle import (
    ProfileLifecycle,
    ProfileLifecyclePage,
    ProfileLifecycleQuery,
    ProfileLifecycleReader,
)
from qs_ai.application.governance.profile_registration import (
    ProfileRegistrar,
    ProfileRegistrationReceipt,
    RegisterProfile,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.transport.grpc.identity import require_qs_workload
from qs_ai.transport.grpc.prompt_drafts import identifier

RECEIPT = TypeAdapter(ProfileRegistrationReceipt)


def reference(value: pb.PromptDraftSource) -> AssetReference:
    return AssetReference(value.identity, value.version, value.fingerprint, value.content_sha256)


def response(value: ProfileRegistrationReceipt) -> pb.ProfileRegistrationReceipt:
    receipt = pb.ProfileRegistrationReceipt(
        schema_version="qs-ai-profile-registration/v1",
        command_id=str(value.command.command_id),
        receipt_json=RECEIPT.dump_json(value).decode(),
    )
    if receipt.ByteSize() > 524288:
        raise ValueError("Profile receipt exceeds limit")
    return receipt


class ProfileManagement(rpc.ProfileManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except AssetConflict:
            await context.abort(grpc.StatusCode.ABORTED, "Profile version or command already used")
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Profile command unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid Profile registration")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; query original command ID"
            )
        raise AssertionError("abort must raise")

    async def Register(
        self, request: pb.ProfileRegisterCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.ProfileRegistrationReceipt:
        async with self.operation(context):
            if request.ByteSize() > 262144:
                raise ValueError("Profile command exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            command = RegisterProfile(
                identifier(request.command_id),
                reference(request.source),
                request.definition_json,
                reference(request.prompt),
                reference(request.generation_route),
                request.reason,
            )
            async with self.container() as operation:
                store = await operation.get(ProfileRegistrar)
                value = await store.register(scope, command, datetime.now(UTC))
            return response(value)
        raise AssertionError("abort must raise")

    async def GetReceipt(
        self, request: pb.ProfileRegistrationQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.ProfileRegistrationReceipt:
        async with self.operation(context):
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(ProfileRegistrar)
                value = await store.get_receipt(scope, identifier(request.command_id))
            return response(value)
        raise AssertionError("abort must raise")

    async def ListLifecycle(
        self, request: pb.ProfileLifecycleQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.AssetCatalogResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192 or request.version:
                raise ValueError("Invalid Profile list query")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            query = ProfileLifecycleQuery(
                request.identity, request.status, request.limit or 20, request.cursor
            )
            async with self.container() as operation:
                reader = await operation.get(ProfileLifecycleReader)
                value = await reader.list(scope, query)
            raw = TypeAdapter(ProfileLifecyclePage).dump_json(value).decode()
            if len(raw.encode()) > 131072:
                raise ValueError("Profile lifecycle page exceeds limit")
            return pb.AssetCatalogResponse(
                schema_version="qs-ai-profile-lifecycle-page/v1", payload_json=raw
            )
        raise AssertionError("abort must raise")

    async def GetLifecycle(
        self, request: pb.ProfileLifecycleQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.AssetCatalogResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192 or request.cursor or request.status or request.limit:
                raise ValueError("Invalid Profile lifecycle query")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                reader = await operation.get(ProfileLifecycleReader)
                value = await reader.get(scope, request.identity, request.version)
            raw = TypeAdapter(ProfileLifecycle).dump_json(value).decode()
            return pb.AssetCatalogResponse(
                schema_version="qs-ai-profile-lifecycle/v1", payload_json=raw
            )
        raise AssertionError("abort must raise")
