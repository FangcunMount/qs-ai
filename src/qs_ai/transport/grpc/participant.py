"""Internal participant execution management, delegated by the QS workload."""

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.execution.management import (
    ParticipantCapacityQuery,
    ParticipantCapacityReader,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
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
