"""Internal QS-delegated governance. Never accepts direct public user identity claims."""

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
from qs_ai.application.evaluation.management import EvaluationManagementStore, ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution


def scope_from(request: pb.EvaluationQuery) -> ManagementScope:
    run_id = UUID(request.run_id)
    if str(run_id) != request.run_id:
        raise ValueError("Canonical Run id required")
    return ManagementScope(run_id, request.organization_id, request.operator_user_id)


class EvaluationManagement(rpc.EvaluationManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        auth = context.auth_context()
        if auth.get("transport_security_type") != [b"ssl"] or auth.get("x509_common_name") != [
            b"qs-apiserver.svc"
        ]:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Untrusted workload")
        try:
            yield
            return
        except CheckpointConflict:
            await context.abort(grpc.StatusCode.ABORTED, "Evaluation version or state changed")
        except NotFound:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Evaluation unavailable")
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid evaluation operation")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Outcome unknown; read evaluation state"
            )
        raise AssertionError("abort must raise")

    async def Get(
        self, request: pb.EvaluationQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request)
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.get(scope)
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")

    async def ResolveUnknown(
        self, request: pb.UnknownResolutionCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            value = ResultUnknownResolution(
                request.execution_id,
                request.decision,
                scope.actor,
                request.reason,
                request.acknowledged_duplicate_call_and_cost_risk,
                datetime.now(UTC),
            )
            if request.expected_version < 1 or not request.confirm:
                raise ValueError("Explicit version and confirmation required")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.resolve(scope, request.expected_version, value, confirm=True)
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")
