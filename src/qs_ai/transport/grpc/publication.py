"""Internal publication management; QS authorizes scope, AI verifies all release evidence."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.governance.publication import (
    MovePublication,
    PublicationReceipt,
    PublicationScope,
    PublicationStore,
    PublishConfiguration,
)
from qs_ai.application.governance.publication_codec import publication_json
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.governance.publication import (
    PublicationConflict,
    PublicationPointer,
    ReleaseSelector,
)
from qs_ai.transport.grpc.identity import require_qs_workload


def canonical_uuid(raw: str) -> UUID:
    value = UUID(raw)
    if value.int == 0 or str(value) != raw:
        raise ValueError("Canonical nonzero UUID required")
    return value


def selector_from(raw: pb.PublicationSelector) -> ReleaseSelector:
    return ReleaseSelector(
        raw.audience,
        raw.model_kind,
        raw.decision_kind,
        raw.model_code if raw.HasField("model_code") else None,
        raw.model_version if raw.HasField("model_version") else None,
    )


def selector_message(selector: ReleaseSelector) -> pb.PublicationSelector:
    return pb.PublicationSelector(
        **{key: value for key, value in asdict(selector).items() if value is not None}
    )


def state_message(pointer: PublicationPointer) -> pb.PublicationState:
    result = pb.PublicationState(
        selector=selector_message(pointer.selector),
        version=pointer.version,
        active_publication_id=str(pointer.active.publication_id) if pointer.active else "",
        publication_json=publication_json(pointer.active) if pointer.active else "",
        changed_at=pointer.changed_at.isoformat() if pointer.changed_at else "",
    )
    if result.ByteSize() > 512 * 1024:
        raise ValueError("Publication state exceeds transport limit")
    return result


def receipt_message(receipt: PublicationReceipt) -> pb.PublicationReceipt:
    change = receipt.change
    result = pb.PublicationReceipt(
        command_id=str(receipt.command_id),
        previous=state_message(change.previous),
        current=state_message(change.current),
        action=change.action,
        actor=change.audit.actor,
        reason=change.audit.reason,
        changed_at=change.audit.at.isoformat(),
    )
    if result.ByteSize() > 1024 * 1024:
        raise ValueError("Publication receipt exceeds transport limit")
    return result


class PublicationManagement(rpc.PublicationManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except (PublicationConflict, CheckpointConflict):
            await context.abort(grpc.StatusCode.ABORTED, "Publication version or command changed")
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Publication or command unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid publication operation")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; query original command ID"
            )
        raise AssertionError("abort must raise")

    async def Publish(
        self,
        request: pb.PublicationPublishCommand,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.PublicationReceipt:
        async with self.operation(context):
            scope, common = command_fields(request)
            command = PublishConfiguration(
                **common,
                run_id=canonical_uuid(request.run_id),
                run_version=request.run_version,
                release_fingerprint=request.release_fingerprint,
            )
            async with self.container() as operation:
                store = await operation.get(PublicationStore)
                receipt = await store.apply(scope, command, datetime.now(UTC))
            return receipt_message(receipt)
        raise AssertionError("abort must raise")

    async def Rollback(
        self,
        request: pb.PublicationRollbackCommand,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.PublicationReceipt:
        async with self.operation(context):
            scope, common = command_fields(request)
            command = MovePublication(
                **common, target_id=canonical_uuid(request.target_publication_id)
            )
            async with self.container() as operation:
                store = await operation.get(PublicationStore)
                receipt = await store.apply(scope, command, datetime.now(UTC))
            return receipt_message(receipt)
        raise AssertionError("abort must raise")

    async def Disable(
        self,
        request: pb.PublicationDisableCommand,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.PublicationReceipt:
        async with self.operation(context):
            scope, common = command_fields(request)
            command = MovePublication(**common, target_id=None)
            async with self.container() as operation:
                store = await operation.get(PublicationStore)
                receipt = await store.apply(scope, command, datetime.now(UTC))
            return receipt_message(receipt)
        raise AssertionError("abort must raise")

    async def Get(
        self,
        request: pb.PublicationQuery,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.PublicationState:
        async with self.operation(context):
            PublicationScope(request.scope.organization_id, request.scope.operator_user_id)
            selector = selector_from(request.selector)
            async with self.container() as operation:
                store = await operation.get(PublicationStore)
                pointer = await store.get(selector)
            return state_message(pointer)
        raise AssertionError("abort must raise")

    async def GetReceipt(
        self,
        request: pb.PublicationReceiptQuery,
        context: aio.ServicerContext[Any, Any],
    ) -> pb.PublicationReceipt:
        async with self.operation(context):
            scope = PublicationScope(request.scope.organization_id, request.scope.operator_user_id)
            command_id = canonical_uuid(request.command_id)
            async with self.container() as operation:
                store = await operation.get(PublicationStore)
                receipt = await store.get_receipt(scope, command_id)
            return receipt_message(receipt)
        raise AssertionError("abort must raise")


def command_fields(
    request: pb.PublicationPublishCommand
    | pb.PublicationRollbackCommand
    | pb.PublicationDisableCommand,
) -> tuple[PublicationScope, dict[str, Any]]:
    scope = PublicationScope(request.scope.organization_id, request.scope.operator_user_id)
    if not request.HasField("expected"):
        raise ValueError("Explicit pointer expectation required")
    expected = request.expected
    return scope, dict(
        command_id=canonical_uuid(request.command_id),
        selector=selector_from(expected.selector),
        expected_version=expected.version,
        expected_active_id=canonical_uuid(expected.active_publication_id)
        if expected.active_publication_id
        else None,
        reason=request.reason,
        confirm=request.confirm,
    )
