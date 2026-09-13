from dataclasses import replace

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.unknowns import UnknownExecutionIndex, UnknownExecutionSummary
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


def index(run_id):
    return UnknownExecutionIndex(
        run_id,
        12,
        "sha256:" + "a" * 64,
        "blocked",
        1,
        True,
        (
            UnknownExecutionSummary(
                "execution:1",
                "invocation:1",
                "semantic",
                "case:1",
                1,
                "candidate:1",
                1,
                "2026-09-13T00:00:00+00:00",
                "2026-09-13T00:01:00+00:00",
                1,
                "semantic_evaluation",
                "provider_result_unknown",
                1,
                2,
                1,
                70,
                True,
            ),
        ),
    )


async def test_unknown_query_preserves_scope_version_stage_and_budget_without_resolution(handler):
    service, store = handler
    request = pb.EvaluationUnknownQuery(scope=command().scope, expected_version=12)
    store.list_unknowns.return_value = index(request.scope.run_id)
    response = await service.ListUnknownExecutions(request, Context())
    scope, version = store.list_unknowns.await_args.args
    assert scope.organization_id == 7 and scope.actor == "user:42" and version == 12
    assert response.version == 12 and response.can_resolve
    target = response.executions[0]
    assert target.candidate_id == "candidate:1" and target.kind == "semantic"
    assert target.target_execution_limit == 2 and target.stage_execution_limit == 70
    assert pb.EvaluationUnknownIndex.FromString(response.SerializeToString()) == response
    store.resolve.assert_not_awaited()
    store.start.assert_not_awaited()


@pytest.mark.parametrize("case", ["scope", "version", "workload", "oversized"])
async def test_unknown_query_is_rejected_before_storage(handler, case):
    service, store = handler
    request = pb.EvaluationUnknownQuery(scope=command().scope, expected_version=12)
    context, expected = Context(), grpc.StatusCode.INVALID_ARGUMENT
    if case == "scope":
        request.scope.operator_user_id = 0
    elif case == "version":
        request.expected_version = 0
    elif case == "oversized":
        request.scope.run_id = "x" * 8192
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.ListUnknownExecutions(request, context)
    assert error.value.args[0] == expected
    store.list_unknowns.assert_not_awaited()


@pytest.mark.parametrize("case", ["count", "bytes"])
async def test_unknown_response_is_bounded(handler, case):
    service, store = handler
    request = pb.EvaluationUnknownQuery(scope=command().scope, expected_version=12)
    value = index(request.scope.run_id)
    target = value.executions[0]
    store.list_unknowns.return_value = (
        replace(value, executions=(target,) * 141)
        if case == "count"
        else replace(value, executions=(replace(target, failure_code="x" * (256 * 1024)),))
    )
    with pytest.raises(Aborted) as error:
        await service.ListUnknownExecutions(request, Context())
    assert error.value.args[0] == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.parametrize(
    "failure,code",
    [
        (CheckpointConflict("private evidence"), grpc.StatusCode.ABORTED),
        (NotFound("private evidence"), grpc.StatusCode.NOT_FOUND),
        (ValueError("private evidence"), grpc.StatusCode.INVALID_ARGUMENT),
        (RuntimeError("private evidence"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_unknown_error_does_not_disclose_internal_evidence(handler, failure, code):
    service, store = handler
    store.list_unknowns.side_effect = failure
    with pytest.raises(Aborted) as error:
        await service.ListUnknownExecutions(
            pb.EvaluationUnknownQuery(
                scope=command().scope,
                expected_version=12,
            ),
            Context(),
        )
    assert error.value.args[0] == code and "private" not in str(error.value)
