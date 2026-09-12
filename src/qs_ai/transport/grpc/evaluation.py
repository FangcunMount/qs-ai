"""Internal QS-delegated governance. Never accepts direct public user identity claims."""

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
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationManagementStore, ManagementScope
from qs_ai.application.evaluation.requests import EvaluationRequests
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.domain.evaluation.review import CandidateHumanReview, SemanticContradictionReview


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
