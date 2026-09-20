"""Trusted QS workload entry for organization-owned semantic Prompt editing."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, fields
from datetime import UTC, datetime
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio
from pydantic import TypeAdapter

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.semantic_drafts import (
    CreateSemanticDraft,
    FreezeSemanticDraft,
    ReviseSemanticDraft,
    SemanticCommand,
    SemanticDrafts,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.semantic_draft import SemanticDraft
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.transport.grpc.identity import require_qs_workload
from qs_ai.transport.grpc.prompt_drafts import identifier
from qs_ai.transport.grpc.solution_input import unique_object

DRAFT = TypeAdapter(SemanticDraft)


def parse(
    raw: str,
    kind: type[CreateSemanticDraft] | type[ReviseSemanticDraft] | type[FreezeSemanticDraft],
) -> SemanticCommand:
    value = json.loads(raw, object_pairs_hook=unique_object)
    if not isinstance(value, dict) or value.keys() != {field.name for field in fields(kind)}:
        raise ValueError("Complete semantic command required")
    for key in ("draft_id", "command_id"):
        value[key] = identifier(value[key])
    if kind is CreateSemanticDraft:
        for key in ("source_prompt", "source_schema"):
            value[key] = FrozenContractRef(**value[key])
    return kind(**value)


def response(value: SemanticDraft) -> pb.SemanticDraftResponse:
    raw = json.dumps(
        {
            "draft": DRAFT.dump_python(value, mode="json"),
            "asset": asdict(value.asset_reference()) if value.state == "frozen" else None,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(raw.encode()) > 262144:
        raise ValueError("Semantic draft response exceeds limit")
    return pb.SemanticDraftResponse(schema_version="qs-ai-semantic-draft/v1", data_json=raw)


class SemanticPromptDrafts(rpc.SemanticPromptDraftsServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Semantic draft unavailable")
        except DraftConflict:
            await context.abort(
                grpc.StatusCode.ABORTED, "Semantic draft changed; reload before continuing"
            )
        except (ValueError, TypeError, KeyError):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Invalid semantic draft or contract"
            )
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; reconcile original command"
            )

    async def write(
        self,
        request: pb.SemanticDraftWrite,
        context: aio.ServicerContext[Any, Any],
        kind: type[CreateSemanticDraft] | type[ReviseSemanticDraft] | type[FreezeSemanticDraft],
    ) -> pb.SemanticDraftResponse:
        async with self.operation(context):
            if request.ByteSize() > 262144:
                raise ValueError("Semantic command exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            command = parse(request.command_json, kind)
            async with self.container() as operation:
                store = await operation.get(SemanticDrafts)
                value = await store.apply(scope, command, datetime.now(UTC))
            return response(value)
        raise AssertionError("abort must raise")

    async def read(
        self, request: pb.SemanticDraftQuery, context: aio.ServicerContext[Any, Any], kind: str
    ) -> pb.SemanticDraftResponse:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Semantic query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(SemanticDrafts)
                if kind == "receipt":
                    value = await store.receipt(scope, identifier(request.command_id))
                elif kind == "validate":
                    if request.revision <= 0:
                        raise ValueError("Explicit validation revision required")
                    value = await store.validate(
                        scope, identifier(request.draft_id), request.revision
                    )
                else:
                    value = await store.get(scope, identifier(request.draft_id), request.revision)
            return response(value)
        raise AssertionError("abort must raise")

    async def Create(
        self, request: pb.SemanticDraftWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.SemanticDraftResponse:
        return await self.write(request, context, CreateSemanticDraft)

    async def Revise(
        self, request: pb.SemanticDraftWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.SemanticDraftResponse:
        return await self.write(request, context, ReviseSemanticDraft)

    async def Freeze(
        self, request: pb.SemanticDraftWrite, context: aio.ServicerContext[Any, Any]
    ) -> pb.SemanticDraftResponse:
        return await self.write(request, context, FreezeSemanticDraft)

    async def Get(
        self, request: pb.SemanticDraftQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SemanticDraftResponse:
        return await self.read(request, context, "get")

    async def Validate(
        self, request: pb.SemanticDraftQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SemanticDraftResponse:
        return await self.read(request, context, "validate")

    async def GetReceipt(
        self, request: pb.SemanticDraftQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.SemanticDraftResponse:
        return await self.read(request, context, "receipt")
