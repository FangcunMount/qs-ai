from dataclasses import replace

import grpc
import pytest

from qs_ai.application.evaluation.management import EvaluationView
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


async def test_cancel_uses_trusted_scope_server_time_and_preserves_audit_response(handler):
    service, store = handler
    request = cancel_command()
    store.cancel.return_value = replace(
        EvaluationView(request.scope.run_id, 8, "canceled", 0, "[]"),
        cancellation_json='{"schema_version":"qs-ai-evaluation-cancellation/v1"}',
    )
    reply = await service.Cancel(request, Context())
    scope, version, reason, at = store.cancel.await_args.args
    assert (scope.actor, version, reason) == ("user:42", 7, "停止后续工作")
    assert at.utcoffset().total_seconds() == 0
    assert store.cancel.await_args.kwargs == {"discard": False, "confirm": True}
    assert reply.cancellation_json == store.cancel.return_value.cancellation_json


@pytest.mark.parametrize("invalid", ["scope", "version", "confirm", "discard", "size", "workload"])
async def test_cancel_rejects_missing_decision_scope_or_trust_before_storage(handler, invalid):
    service, store = handler
    request, context = cancel_command(), Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
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
