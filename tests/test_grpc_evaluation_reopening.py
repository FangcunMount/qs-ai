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


def reopen_command():
    return pb.EvaluationReopenCommand(
        scope=command().scope, expected_version=13, reason="复核语义判断分歧", confirm=True
    )


async def test_reopen_uses_trusted_actor_and_server_time_and_returns_history(handler):
    service, store = handler
    request = reopen_command()
    at = datetime.now(UTC)
    view = EvaluationView(
        request.scope.run_id, 14, "awaiting_review", 0, "[]", reopenings_json='[{"version":14}]'
    )
    store.reopen.return_value = view
    response = await service.ReopenReview(request, Context())
    scope, version, reason, timestamp = store.reopen.await_args.args
    assert scope.actor == "user:42" and scope.organization_id == 7
    assert version == 13 and reason == request.reason
    assert at <= timestamp <= datetime.now(UTC)
    assert store.reopen.await_args.kwargs == {"confirm": True}
    assert response.reopenings_json == view.reopenings_json
    assert pb.EvaluationState.FromString(response.SerializeToString()) == response


@pytest.mark.parametrize("case", ["scope", "version", "confirm", "workload"])
async def test_invalid_reopen_never_reaches_store(handler, case):
    service, store = handler
    request, context = reopen_command(), Context()
    code = grpc.StatusCode.INVALID_ARGUMENT
    if case == "scope":
        request.scope.operator_user_id = 0
    elif case == "version":
        request.expected_version = 0
    elif case == "confirm":
        request.confirm = False
    else:
        context.auth_context = lambda: {
            "transport_security_type": [b"ssl"],
            "x509_common_name": [b"other.svc"],
        }
        code = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.ReopenReview(request, context)
    assert error.value.args[0] == code
    store.reopen.assert_not_awaited()


@pytest.mark.parametrize(
    "error,code",
    [
        (CheckpointConflict("private"), grpc.StatusCode.ABORTED),
        (NotFound("private"), grpc.StatusCode.NOT_FOUND),
        (ValueError("private"), grpc.StatusCode.INVALID_ARGUMENT),
        (RuntimeError("private"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_reopen_failure_is_redacted_without_retry(handler, error, code):
    service, store = handler
    store.reopen.side_effect = error
    with pytest.raises(Aborted) as caught:
        await service.ReopenReview(reopen_command(), Context())
    assert caught.value.args[0] == code and "private" not in str(caught.value)
    assert store.reopen.await_count == 1
