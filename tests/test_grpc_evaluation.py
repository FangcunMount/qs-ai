from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationManagementStore, EvaluationView
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.test_grpc_commands import Aborted, Context


@pytest.fixture
def handler():
    store = AsyncMock(spec=EvaluationManagementStore)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return store

        yield Scope()

    return EvaluationManagement(container), store


def command():
    return pb.UnknownResolutionCommand(
        scope=pb.EvaluationQuery(run_id=str(uuid4()), organization_id=7, operator_user_id=42),
        expected_version=6,
        execution_id="execution:1",
        decision="authorize_replacement",
        reason="人工核对",
        confirm=True,
        acknowledged_duplicate_call_and_cost_risk=True,
    )


async def test_read_returns_original_creation_receipt_without_mutation(handler):
    service, store = handler
    scope = command().scope
    store.get.return_value = replace(
        EvaluationView(scope.run_id, 7, "collecting", 0, "[]"),
        creation_json='{"schema_version":"qs-ai-evaluation-creation-receipt/v1"}',
    )
    reply = await service.Get(scope, Context())
    assert reply.creation_json == store.get.return_value.creation_json
    assert reply.version == 7
    store.start.assert_not_awaited()
    store.resolve.assert_not_awaited()


async def test_only_trusted_qs_workload_can_reach_store(handler):
    service, store = handler

    class Untrusted(Context):
        def auth_context(self):
            return {"transport_security_type": [b"ssl"], "x509_common_name": [b"other.svc"]}

    for method, request in ((service.Get, command().scope), (service.ResolveUnknown, command())):
        with pytest.raises(Aborted) as error:
            await method(request, Untrusted())
        assert error.value.args[0] == grpc.StatusCode.PERMISSION_DENIED
    store.get.assert_not_awaited()
    store.resolve.assert_not_awaited()


@pytest.mark.parametrize(
    "field", ["organization_id", "operator_user_id", "confirm", "risk", "version"]
)
async def test_missing_scope_and_confirmations_rejected_before_store(handler, field):
    service, store = handler
    request = command()
    if field in ("organization_id", "operator_user_id"):
        setattr(request.scope, field, 0)
    elif field == "confirm":
        request.confirm = False
    elif field == "risk":
        request.acknowledged_duplicate_call_and_cost_risk = False
    else:
        request.expected_version = 0
    with pytest.raises(Aborted) as error:
        await service.ResolveUnknown(request, Context())
    assert error.value.args[0] == grpc.StatusCode.INVALID_ARGUMENT
    store.resolve.assert_not_awaited()


async def test_resolution_uses_trusted_operator_and_server_time(handler):
    service, store = handler
    request = command()
    store.resolve.return_value = EvaluationView(request.scope.run_id, 7, "collecting", 0, "[]")
    reply = await service.ResolveUnknown(request, Context())
    assert reply.version == 7
    scope, version, value = store.resolve.await_args.args
    assert (scope.organization_id, scope.operator_user_id, value.actor) == (7, 42, "user:42")
    assert value.resolved_at.utcoffset().total_seconds() == 0
    assert version == 6 and store.resolve.await_args.kwargs == {"confirm": True}


@pytest.mark.parametrize(
    "failure, code",
    [
        (CheckpointConflict("secret"), grpc.StatusCode.ABORTED),
        (NotFound("secret"), grpc.StatusCode.NOT_FOUND),
        (ValueError("secret"), grpc.StatusCode.INVALID_ARGUMENT),
        (RuntimeError("secret"), grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_error_responses_do_not_expose_internal_data(handler, failure, code):
    service, store = handler
    store.resolve.side_effect = failure
    with pytest.raises(Aborted) as error:
        await service.ResolveUnknown(command(), Context())
    assert error.value.args[0] == code
    assert "secret" not in str(error.value)


def test_governance_registration_is_disabled_in_production_defaults():
    assert not Settings(environment="production").grpc.governance_enabled


async def test_start_uses_server_time_and_trusted_actor(handler):
    service, store = handler
    request = pb.EvaluationStartCommand(
        scope=command().scope, expected_version=1, reason="启动", confirm=True
    )
    store.start.return_value = EvaluationView(request.scope.run_id, 2, "collecting", 0, "[]")
    reply = await service.Start(request, Context())
    assert reply.status == "collecting" and reply.version == 2
    scope, version, reason, at = store.start.await_args.args
    assert scope.actor == "user:42" and version == 1 and reason == "启动"
    assert at.utcoffset().total_seconds() == 0
    assert store.start.await_args.kwargs == {"confirm": True}


@pytest.mark.parametrize("invalid", ["confirm", "version", "scope", "workload"])
async def test_start_rejected_before_store(handler, invalid):
    service, store = handler
    request = pb.EvaluationStartCommand(
        scope=command().scope, expected_version=1, reason="启动", confirm=True
    )
    context = Context()
    expected = grpc.StatusCode.INVALID_ARGUMENT
    if invalid == "confirm":
        request.confirm = False
    elif invalid == "version":
        request.expected_version = 0
    elif invalid == "scope":
        request.scope.operator_user_id = 0
    else:
        context.auth_context = lambda: {}
        expected = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(Aborted) as error:
        await service.Start(request, context)
    assert error.value.args[0] == expected
    store.start.assert_not_awaited()
