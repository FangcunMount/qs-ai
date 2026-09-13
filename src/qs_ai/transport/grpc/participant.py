"""Internal participant execution management, delegated by the QS workload."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.execution.management import (
    ParticipantCapacityQuery,
    ParticipantCapacityReader,
)
from qs_ai.application.execution.retry import (
    ParticipantRetry,
    ParticipantRetryStore,
    RetryParticipant,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import AccessDenied, NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.transport.grpc.commands import receipt_message
from qs_ai.transport.grpc.identity import require_qs_workload


class ParticipantManagement(rpc.ParticipantManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    async def GetCapacity(
        self, request: pb.ParticipantCapacityQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.ParticipantCapacitySnapshot:
        await require_qs_workload(context)
        try:
            if request.ByteSize() > 1024:
                raise ValueError("Capacity query exceeds limit")
            query = ParticipantCapacityQuery(
                DraftScope(request.scope.organization_id, request.scope.operator_user_id),
                request.subject_id,
                request.assessment_id,
            )
            async with self.container() as operation:
                reader = await operation.get(ParticipantCapacityReader)
                result = await reader.get(query, datetime.now(UTC))
            payload = {key: value for key, value in asdict(result).items() if value is not None}
            return pb.ParticipantCapacitySnapshot(**payload)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid capacity query")
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Participant capacity unavailable")
        raise AssertionError("abort must raise")

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except AccessDenied:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Participant access revoked")
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Participant record not found")
        except RuleViolation as error:
            code = (
                grpc.StatusCode.RESOURCE_EXHAUSTED
                if error.code == "participant_daily_capacity_exceeded"
                else grpc.StatusCode.ABORTED
            )
            await context.abort(code, error.code)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid participant command")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; query original command"
            )
        raise AssertionError("abort must raise")

    async def GetExecution(
        self, request: pb.ParticipantExecutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.ParticipantExecution:
        async with self.operation(context):
            if request.ByteSize() > 1024 or str(UUID(request.session_id)) != request.session_id:
                raise ValueError("Invalid session query")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                store = await operation.get(ParticipantRetryStore)
                result = await store.get(scope, request.session_id)
            return pb.ParticipantExecution(**asdict(result))
        raise AssertionError("abort must raise")

    async def Retry(
        self, request: pb.ParticipantRetryCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.Receipt:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Retry command exceeds limit")
            command = ParticipantRetry(
                DraftScope(request.scope.organization_id, request.scope.operator_user_id),
                request.session_id,
                request.command_id,
                request.expected_run_id,
                request.expected_version,
                request.reason,
                request.confirm,
                request.expected_provider_invocations,
                request.accept_result_unknown_risk,
            )
            async with self.container() as operation:
                service = await operation.get(RetryParticipant)
                result = await service.execute(command)
            return receipt_message(result)
        raise AssertionError("abort must raise")

    async def GetRetryReceipt(
        self, request: pb.ParticipantRetryReceiptQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.Receipt:
        async with self.operation(context):
            if request.ByteSize() > 1024 or str(UUID(request.command_id)) != request.command_id:
                raise ValueError("Invalid command query")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            async with self.container() as operation:
                service = await operation.get(RetryParticipant)
                result = await service.receipt(scope, request.command_id)
            return receipt_message(result)
        raise AssertionError("abort must raise")
