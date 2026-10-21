"""Real RPC/adapter boundary with a stub dispatcher; no SDK or native DB claim."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import grpc
import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationManagementStore, EvaluationView
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation_finalization import final_command
from tests.test_grpc_evaluation_reopening import reopen_command


def fixture(kind):
    request = final_command() if kind == "finalize" else reopen_command()
    view = EvaluationView(
        request.scope.run_id,
        request.expected_version + 1,
        "rejected" if kind == "finalize" else "awaiting_review",
        0,
        "[]",
    )
    tx = SimpleNamespace(open=Mock(side_effect=AssertionError("No legacy database root permitted")))
    original = MySQLEvaluationManagement(tx)
    dispatch = AsyncMock(return_value=view)
    new_id = Mock(side_effect=uuid4)
    store = TransactionalEvaluationManagement(original, dispatch, new_id=new_id)

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return store

        yield Scope()

    service = EvaluationManagement(container)
    method = service.Finalize if kind == "finalize" else service.ReopenReview
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
async def test_real_rpc_reaches_authorized_frozen_operation_with_server_time(kind):
    f = fixture(kind)
    assert f.dispatch.await_count == 0 and f.tx.open.call_count == 0
    before = datetime.now(UTC)
    reply = await f.method(f.request, Context())
    operation, at = f.dispatch.await_args.args
    assert operation.kind == kind and operation.confirm is True
    assert operation.scope.actor == "user:42" and operation.scope.organization_id == 7
    assert str(operation.scope.run_id) == f.request.scope.run_id
    assert operation.expected_version == f.request.expected_version
    assert operation.reason == f.request.reason
    assert operation.expected_passed is (False if kind == "finalize" else None)
    assert before <= at <= datetime.now(UTC)
    assert reply.version == f.view.version and reply.status == f.view.status
    assert f.new_id.call_count == 1 and f.dispatch.await_count == 1
    f.tx.open.assert_not_called()
    assert f.store.transactions is f.original.transactions
    assert f.store.runtime_limits is f.original.runtime_limits


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
async def test_untrusted_rpc_cannot_generate_operation_or_prepare_half(kind):
    f = fixture(kind)
    context = Context()
    context.auth_context = lambda: {}
    with pytest.raises(Aborted) as error:
        await f.method(f.request, context)
    assert error.value.args[0] == grpc.StatusCode.PERMISSION_DENIED
    f.dispatch.assert_not_called()
    f.new_id.assert_not_called()
    f.tx.open.assert_not_called()


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
async def test_new_public_invocation_keeps_existing_cas_contract_without_implicit_replay(kind):
    f = fixture(kind)
    await f.method(f.request, Context())
    original_operation = f.dispatch.await_args.args[0]
    f.dispatch.side_effect = CheckpointConflict("private original version changed")
    with pytest.raises(Aborted) as error:
        await f.method(f.request, Context())
    assert error.value.args[0] == grpc.StatusCode.ABORTED
    assert "private" not in str(error.value)
    repeated = f.dispatch.await_args.args[0]
    assert repeated.operation_id != original_operation.operation_id
    assert repeated.scope == original_operation.scope
    assert repeated.expected_version == original_operation.expected_version
    assert f.dispatch.await_count == f.new_id.call_count == 2
    f.tx.open.assert_not_called()


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
async def test_missing_confirmation_cannot_reach_transaction_dispatch(kind):
    f = fixture(kind)
    f.request.confirm = False
    with pytest.raises(Aborted) as error:
        await f.method(f.request, Context())
    assert error.value.args[0] == grpc.StatusCode.INVALID_ARGUMENT
    f.dispatch.assert_not_called()
    f.new_id.assert_not_called()
