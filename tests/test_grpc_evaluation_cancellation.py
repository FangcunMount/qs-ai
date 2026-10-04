import grpc
import pytest

from qs_ai.contracts.workflow import workflow_pb2 as pb
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation import command
from tests.test_grpc_evaluation import handler as handler


def cancel_command():
    return pb.EvaluationCancelCommand(
        scope=command().scope,
        expected_version=7,
        reason="停止后续工作",
        confirm=True,
        discard=False,
    )


@pytest.mark.parametrize("invalid", ["scope", "version", "confirm", "discard", "size", "workload"])
async def test_cancel_rejects_missing_decision_scope_or_trust_before_storage(handler, invalid):
    service, store = handler
    request, context = cancel_command(), Context()
    expected = grpc.StatusCode.FAILED_PRECONDITION
    if invalid == "scope":
        request.scope.operator_user_id = 0
    elif invalid == "version":
        request.expected_version = 0
    elif invalid == "confirm":
        request.confirm = False
    elif invalid == "discard":
        request.ClearField("discard")
    elif invalid == "size":
        request.reason = "x" * 8192
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.Cancel(request, context)
    assert error.value.args[0] == expected
    store.cancel.assert_not_awaited()
