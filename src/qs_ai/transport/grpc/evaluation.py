"""Internal QS-delegated governance. Never accepts direct public user identity claims."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, fields
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.evaluation.candidates import validate_candidate_query
from qs_ai.application.evaluation.capacity import CapacityExceeded, EvaluationCapacityReader
from qs_ai.application.evaluation.catalog import EvaluationCatalog, EvaluationCatalogQuery
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.diagnostics import EvaluationDiagnostics, ExecutionQuery
from qs_ai.application.evaluation.management import EvaluationManagementStore, ManagementScope
from qs_ai.application.evaluation.planning import EvaluationPlanner, EvaluationPlanQuery
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.application.evaluation.unknowns import validate_unknown_query
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.domain.evaluation.review import CandidateHumanReview, SemanticContradictionReview
from qs_ai.transport.grpc.identity import require_qs_workload


def scope_from(request: pb.EvaluationQuery) -> ManagementScope:
    run_id = UUID(request.run_id)
    if str(run_id) != request.run_id:
        raise ValueError("Canonical Run id required")
    return ManagementScope(run_id, request.organization_id, request.operator_user_id)


class EvaluationManagement(rpc.EvaluationManagementServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    async def GetCapacity(
        self, request: pb.PublicationScope, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationCapacitySnapshot:
        async with self.operation(context):
            scope = DraftScope(request.organization_id, request.operator_user_id)
            async with self.container() as operation:
                reader = await operation.get(EvaluationCapacityReader)
                value = await reader.get(scope, datetime.now(UTC))
            return pb.EvaluationCapacitySnapshot(**asdict(value))
        raise AssertionError("abort must raise")

    async def List(
        self, request: pb.EvaluationCatalogQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationCatalogPage:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Evaluation catalog query exceeds limit")
            query = EvaluationCatalogQuery(
                request.scope.organization_id,
                request.scope.operator_user_id,
                request.status,
                request.limit or 20,
                request.cursor,
            )
            async with self.container() as operation:
                catalog = await operation.get(EvaluationCatalog)
                page = await catalog.list(query)
            return pb.EvaluationCatalogPage(
                items=[pb.EvaluationSummary(**asdict(item)) for item in page.items],
                next_cursor=page.next_cursor,
            )
        raise AssertionError("abort must raise")

    async def ListExecutions(
        self, request: pb.EvaluationExecutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationExecutionPage:
        async with self.operation(context):
            if request.ByteSize() > 8192 or request.execution_id:
                raise ValueError("Invalid execution list query")
            query = ExecutionQuery(
                scope_from(request.scope),
                request.expected_version,
                request.cursor,
                request.limit or 20,
            )
            async with self.container() as operation:
                reader = await operation.get(EvaluationDiagnostics)
                page = await reader.list(query)
            response = pb.EvaluationExecutionPage(**asdict(page))
            if response.ByteSize() > 1024 * 1024:
                raise ValueError("Execution page exceeds read bound")
            return response
        raise AssertionError("abort must raise")

    async def GetExecutionOutput(
        self, request: pb.EvaluationExecutionQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationExecutionOutput:
        async with self.operation(context):
            if request.ByteSize() > 8192 or request.cursor or request.limit:
                raise ValueError("Invalid execution output query")
            query = ExecutionQuery(scope_from(request.scope), request.expected_version)
            async with self.container() as operation:
                reader = await operation.get(EvaluationDiagnostics)
                output = await reader.get(query, request.execution_id)
            response = pb.EvaluationExecutionOutput(**asdict(output))
            if response.ByteSize() > 1024 * 1024:
                raise ValueError("Execution output exceeds read bound")
            return response
        raise AssertionError("abort must raise")

    async def Prepare(
        self, request: pb.EvaluationPlanQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationPlan:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Evaluation plan query exceeds limit")
            scope = DraftScope(request.scope.organization_id, request.scope.operator_user_id)
            refs = [
                FrozenContractRef(v.id, v.version, v.fingerprint)
                for v in (request.suite, request.generation_route, request.semantic_route)
            ]
            async with self.container() as operation:
                planner = await operation.get(EvaluationPlanner)
                plan = await planner.prepare(EvaluationPlanQuery(scope, *refs))
            raw = json.dumps(asdict(plan), ensure_ascii=False, separators=(",", ":"))
            response = pb.EvaluationPlan(schema_version="qs-ai-evaluation-plan/v1", plan_json=raw)
            if response.ByteSize() > 32768:
                raise ValueError("Evaluation plan exceeds limit")
            return response
        raise AssertionError("abort must raise")

    @asynccontextmanager
    async def operation(self, context: aio.ServicerContext[Any, Any]) -> AsyncIterator[None]:
        await require_qs_workload(context)
        try:
            yield
            return
        except CapacityExceeded:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Evaluation capacity exhausted")
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

    async def Create(
        self, request: pb.EvaluationCreateCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if not request.confirm:
                raise ValueError("Explicit confirmation required")
            refs = {}
            for field in fields(EvidenceReleaseIdentity):
                value = getattr(request.release, field.name)
                refs[field.name] = FrozenContractRef(value.id, value.version, value.fingerprint)
            release = EvidenceReleaseIdentity(**refs)
            async with self.container() as operation:
                requests = await operation.get(EvaluationRequests)
                view = await requests.create(
                    scope, release, request.reason, datetime.now(UTC), confirm=True
                )
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")

    async def Start(
        self, request: pb.EvaluationStartCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if request.expected_version < 1 or not request.confirm:
                raise ValueError("Explicit version and confirmation required")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.start(
                    scope, request.expected_version, request.reason, datetime.now(UTC), confirm=True
                )
            return pb.EvaluationState(**asdict(view))
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

    async def Cancel(
        self, request: pb.EvaluationCancelCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if (
                request.ByteSize() > 8192
                or request.expected_version < 1
                or not request.confirm
                or not request.HasField("discard")
            ):
                raise ValueError("Explicit version, discard decision and confirmation required")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.cancel(
                    scope,
                    request.expected_version,
                    request.reason,
                    datetime.now(UTC),
                    discard=request.discard,
                    confirm=True,
                )
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")

    async def ListCandidates(
        self, request: pb.EvaluationQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationCandidateIndex:
        async with self.operation(context):
            scope = scope_from(request)
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.list_candidates(scope)
            return pb.EvaluationCandidateIndex(**asdict(view))
        raise AssertionError("abort must raise")

    async def GetCandidate(
        self, request: pb.EvaluationCandidateQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationCandidateEvidence:
        async with self.operation(context):
            scope = scope_from(request.scope)
            validate_candidate_query(request.candidate_id, request.expected_version)
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.get_candidate(
                    scope, request.candidate_id, request.expected_version
                )
            return pb.EvaluationCandidateEvidence(**asdict(view))
        raise AssertionError("abort must raise")

    async def Review(
        self, request: pb.EvaluationReviewCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if request.expected_version < 1 or not 1 <= len(request.reviews) <= 35:
                raise ValueError("Explicit version and bounded review batch required")
            at = datetime.now(UTC)
            values = []
            for item in request.reviews:
                semantic = None
                if item.HasField("semantic_review"):
                    raw = item.semantic_review
                    semantic = SemanticContradictionReview(
                        raw.policy_version,
                        raw.execution_id,
                        raw.output_fingerprint,
                        raw.assertion_ordinal,
                        raw.original_detail,
                        raw.candidate_excerpt,
                        raw.reason.strip(),
                    )
                values.append(
                    CandidateHumanReview(
                        item.candidate_id.strip(),
                        request.role,
                        scope.actor,
                        item.decision,
                        at,
                        item.reason.strip(),
                        semantic,
                    )
                )
            if len({v.candidate_id for v in values}) != len(values):
                raise ValueError("Duplicate review candidate")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.review(scope, request.expected_version, tuple(values))
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")

    async def PreviewGates(
        self, request: pb.EvaluationGateQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationGatePreview:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if request.expected_version < 1:
                raise ValueError("Explicit Run version required")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.preview_gates(scope, request.expected_version, datetime.now(UTC))
            body = {
                **asdict(view.quality),
                "schema_version": "qs-ai-evaluation-gate-preview/v1",
                "evaluated_at": view.quality.evaluated_at.isoformat(),
                "gate_passes": dict(view.gate_passes),
            }
            payload = json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            if len(payload.encode()) > 256 * 1024:
                raise ValueError("Gate preview exceeds response bound")
            return pb.EvaluationGatePreview(
                run_id=view.run_id,
                version=view.version,
                release_fingerprint=view.release_fingerprint,
                gate_result_json=payload,
            )
        raise AssertionError("abort must raise")

    async def Finalize(
        self, request: pb.EvaluationFinalizeCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if (
                request.expected_version < 1
                or not request.confirm
                or not request.HasField("expected_passed")
            ):
                raise ValueError("Explicit version, expected outcome and confirmation required")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.finalize(
                    scope,
                    request.expected_version,
                    request.expected_passed,
                    request.reason,
                    datetime.now(UTC),
                    confirm=True,
                )
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")

    async def ReopenReview(
        self, request: pb.EvaluationReopenCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationState:
        async with self.operation(context):
            scope = scope_from(request.scope)
            if request.expected_version < 1 or not request.confirm:
                raise ValueError("Explicit version and confirmation required")
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.reopen(
                    scope, request.expected_version, request.reason, datetime.now(UTC), confirm=True
                )
            return pb.EvaluationState(**asdict(view))
        raise AssertionError("abort must raise")

    async def ListUnknownExecutions(
        self, request: pb.EvaluationUnknownQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EvaluationUnknownIndex:
        async with self.operation(context):
            if request.ByteSize() > 8192:
                raise ValueError("Unknown-call query exceeds limit")
            scope = scope_from(request.scope)
            validate_unknown_query(request.expected_version)
            async with self.container() as operation:
                store = await operation.get(EvaluationManagementStore)
                view = await store.list_unknowns(scope, request.expected_version)
            response = pb.EvaluationUnknownIndex(**asdict(view))
            if len(response.executions) > 140 or response.ByteSize() > 256 * 1024:
                raise ValueError("Unknown-call evidence exceeds response bound")
            return response
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
