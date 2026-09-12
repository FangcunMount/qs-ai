"""Trusted QS draft operations; saving does not validate, approve or publish."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import grpc
from dishka import AsyncContainer
from grpc import aio
from pydantic import TypeAdapter

from qs_ai.application.governance.prompt_drafts import (
    CreatePromptDraft,
    DraftScope,
    PromptDraftStore,
    RevisePromptDraft,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt_draft import DraftConflict, PromptDraft, PromptDraftContent
from qs_ai.transport.grpc.identity import require_qs_workload

SNAPSHOT = TypeAdapter(PromptDraft)


def identifier(raw: str) -> UUID:
    value = UUID(raw)
    if value.int == 0 or str(value) != raw:
        raise ValueError("Canonical nonzero UUID required")
    return value


def response(draft: PromptDraft) -> pb.PromptDraftState:
    state = pb.PromptDraftState(
        schema_version="qs-ai-prompt-draft/v1",
        draft_id=str(draft.draft_id),
        revision=draft.revision,
        snapshot_json=SNAPSHOT.dump_json(draft).decode(),
    )
    if state.ByteSize() > 262144:
        raise ValueError("Draft response exceeds limit")
    return state


class PromptDraftManagement(rpc.PromptDraftManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except DraftConflict:
            await context.abort(grpc.StatusCode.ABORTED, "Draft revision or command changed")
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Draft revision or command unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid Prompt draft operation")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; query original command ID"
            )
        raise AssertionError("abort must raise")

    async def Create(
        self, request: pb.PromptDraftCreateCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.PromptDraftState:
        async with self.operation(context):
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            source = request.source
            command = CreatePromptDraft(
                identifier(request.draft_id),
                identifier(request.command_id),
                AssetReference(
                    source.identity, source.version, source.fingerprint, source.content_sha256
                ),
                request.template_id,
                request.target_version,
                request.reason,
            )
            async with self.container() as operation:
                store = await operation.get(PromptDraftStore)
                saved = await store.apply(scope, command, datetime.now(UTC))
            return response(saved)
        raise AssertionError("abort must raise")

    async def Revise(
        self, request: pb.PromptDraftReviseCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.PromptDraftState:
        async with self.operation(context):
            if not request.HasField("content") or request.ByteSize() > 262144:
                raise ValueError("Explicit bounded draft content required")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            content = request.content
            command = RevisePromptDraft(
                identifier(request.draft_id),
                identifier(request.command_id),
                request.expected_revision,
                PromptDraftContent(
                    content.system_message,
                    content.task_template,
                    content.data_preamble,
                    tuple(content.allowed_placeholders),
                ),
                request.reason,
            )
            async with self.container() as operation:
                store = await operation.get(PromptDraftStore)
                saved = await store.apply(scope, command, datetime.now(UTC))
            return response(saved)
        raise AssertionError("abort must raise")

    async def Get(
        self, request: pb.PromptDraftQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.PromptDraftState:
        async with self.operation(context):
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            revision = request.revision if request.HasField("revision") else None
            if revision is not None and revision < 1:
                raise ValueError("Positive revision required")
            async with self.container() as operation:
                store = await operation.get(PromptDraftStore)
                saved = await store.get(scope, identifier(request.draft_id), revision)
            return response(saved)
        raise AssertionError("abort must raise")

    async def GetReceipt(
        self, request: pb.PromptDraftReceiptQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.PromptDraftState:
        async with self.operation(context):
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(PromptDraftStore)
                saved = await store.get_receipt(scope, identifier(request.command_id))
            return response(saved)
        raise AssertionError("abort must raise")
