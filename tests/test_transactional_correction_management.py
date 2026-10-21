"""Actual CorrectReview RPC preserves the supplied command identity and server clock."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.management import EvaluationManagementStore, EvaluationView
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_review_correction import request


def fixture():
    value = request()
    tx = SimpleNamespace(open=Mock(side_effect=AssertionError("No legacy root")))
    dispatch = AsyncMock(
        return_value=EvaluationView(value.scope.run_id, 18, "awaiting_review", 0, "[]")
    )
    new_id = Mock(side_effect=AssertionError("Existing command identity cannot be replaced"))
    store = TransactionalEvaluationManagement(
        MySQLEvaluationManagement(tx), dispatch, new_id=new_id
    )

    @asynccontextmanager
    async def container():
        class Scope:
            async def get(self, dependency):
                assert dependency is EvaluationManagementStore
                return store

        yield Scope()

    service = EvaluationManagement(container)
    return SimpleNamespace(**locals())


async def test_real_correction_handler_preserves_command_input_and_server_audit_time():
    f = fixture()
    before = datetime.now(UTC)
    reply = await f.service.CorrectReview(f.value, Context())
    operation, at = f.dispatch.await_args.args
    assert isinstance(operation, CorrectionStateOperation)
    assert operation.command_id == f.value.command_id and operation.expected_version == 17
    assert operation.value.reviewer == operation.scope.actor == "user:42"
    assert operation.value.reason == f.value.reason.strip()
    assert operation.value.reviewed_at == at and before <= at <= datetime.now(UTC)
    assert reply.version == 18
    f.new_id.assert_not_called()
    f.tx.open.assert_not_called()


@pytest.mark.parametrize("damage", ["workload", "confirm", "command"])
async def test_invalid_correction_never_dispatches_or_allocates_new_identity(damage):
    f = fixture()
    context = Context()
    if damage == "workload":
        context.auth_context = lambda: {}
    elif damage == "confirm":
        f.value.confirm = False
    else:
        f.value.command_id = "invalid"
    with pytest.raises(Aborted):
        await f.service.CorrectReview(f.value, context)
    f.dispatch.assert_not_called()
    f.new_id.assert_not_called()
    f.tx.open.assert_not_called()
