from datetime import UTC, datetime

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import workflow_pb2 as pb
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


def final_command():
    return pb.EvaluationFinalizeCommand(
        scope=command().scope,
        expected_version=12,
        expected_passed=False,
        reason="核对门槛后拒绝",
        confirm=True,
    )


async def test_finalize_binds_actor_version_explicit_false_and_server_time(handler):
    service, store = handler
    request = final_command()
    at = datetime.now(UTC)
    view = EvaluationView(request.scope.run_id, 13, "rejected", 0, "[]", "[]", '{"passed":false}')
    store.finalize.return_value = view
    response = await service.Finalize(request, Context())
    scope, version, passed, reason, timestamp = store.finalize.await_args.args
    assert scope.actor == "user:42" and scope.organization_id == 7
    assert version == 12 and passed is False and reason == request.reason
    assert at <= timestamp <= datetime.now(UTC)
    assert store.finalize.await_args.kwargs == {"confirm": True}
    assert response.finalization_json == view.finalization_json
    assert pb.EvaluationState.FromString(response.SerializeToString()) == response


@pytest.mark.parametrize("case", ["scope", "version", "confirm", "missing_outcome", "workload"])
async def test_bad_finalize_request_never_reaches_store(handler, case):
    service, store = handler
    request, context = final_command(), Context()
    code = grpc.StatusCode.INVALID_ARGUMENT
    if case == "scope":
        request.scope.organization_id = 0
    elif case == "version":
        request.expected_version = 0
    elif case == "confirm":
        request.confirm = False
    elif case == "missing_outcome":
        request.ClearField("expected_passed")
    else:
        context.auth_context = lambda: {}
        code = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.Finalize(request, context)
    assert error.value.args[0] == code
    store.finalize.assert_not_awaited()


@pytest.mark.parametrize(
    "error,code",
    [
        (CheckpointConflict("private"), grpc.StatusCode.ABORTED),
        (NotFound("private"), grpc.StatusCode.NOT_FOUND),
        (ValueError("private"), grpc.StatusCode.INVALID_ARGUMENT),
        (RuntimeError("private"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_finalize_errors_are_redacted_and_never_retried(handler, error, code):
    service, store = handler
    store.finalize.side_effect = error
    with pytest.raises(Aborted) as caught:
        await service.Finalize(final_command(), Context())
    assert caught.value.args[0] == code and "private" not in str(caught.value)
    assert store.finalize.await_count == 1
