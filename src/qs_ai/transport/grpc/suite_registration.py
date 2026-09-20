"""Trusted QS registration of immutable suite versions and scoped command receipts."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio
from pydantic import TypeAdapter

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.suite_registration import (
    RegisterSuite,
    SuiteRegistrar,
    SuiteRegistrationReceipt,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import AssetConflict
from qs_ai.transport.grpc.identity import require_qs_workload
from qs_ai.transport.grpc.prompt_drafts import identifier

RECEIPT = TypeAdapter(SuiteRegistrationReceipt)


def reference(value: pb.PromptDraftSource) -> AssetReference:
    return AssetReference(value.identity, value.version, value.fingerprint, value.content_sha256)


def response(value: SuiteRegistrationReceipt) -> pb.SuiteRegistrationReceipt:
    receipt = pb.SuiteRegistrationReceipt(
        schema_version="qs-ai-suite-registration/v1",
        command_id=str(value.command.command_id),
        receipt_json=RECEIPT.dump_json(value, exclude_defaults=True).decode(),
    )
    if receipt.ByteSize() > 32768:
        raise ValueError("Suite receipt exceeds limit")
    return receipt


class SuiteManagement(rpc.SuiteManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except AssetConflict:
            await context.abort(grpc.StatusCode.ABORTED, "Suite version or command already used")
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Suite command unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid Suite registration")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; query original command ID"
            )
        raise AssertionError("abort must raise")

    async def Register(
        self, request: pb.SuiteRegisterCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.SuiteRegistrationReceipt:
        async with self.operation(context):
            if request.ByteSize() > 16384:
                raise ValueError("Suite command exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            command = RegisterSuite(
                identifier(request.command_id),
                FrozenContractRef(
                    request.source.id, request.source.version, request.source.fingerprint
                ),
                request.suite_id,
                request.suite_version,
                reference(request.profile),
                reference(request.prompt),
                reference(request.generation_route),
                request.reason,
                case_edits_json=request.case_edits_json,
                semantic_prompt=FrozenContractRef(
                    request.semantic_prompt.id,
                    request.semantic_prompt.version,
                    request.semantic_prompt.fingerprint,
                )
                if request.HasField("semantic_prompt")
                else None,
                semantic_owner_organization_id=request.semantic_owner_organization_id,
            )
            async with self.container() as operation:
                store = await operation.get(SuiteRegistrar)
                value = await store.register(scope, command, datetime.now(UTC))
            return response(value)
        raise AssertionError("abort must raise")

    async def GetReceipt(
        self, request: pb.SuiteRegistrationQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SuiteRegistrationReceipt:
        async with self.operation(context):
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(SuiteRegistrar)
                value = await store.get_receipt(scope, identifier(request.command_id))
            return response(value)
        raise AssertionError("abort must raise")
