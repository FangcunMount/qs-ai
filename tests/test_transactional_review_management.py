"""Actual Review RPC reaches the explicit adapter; no native DB/Broker claim."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import EvaluationManagementStore, EvaluationView
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.workflow_transport.review_state_plan import ReviewStateInput
from qs_ai.infrastructure.workflow_transport.transactional_evaluation_management import (
    TransactionalEvaluationManagement,
)
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.test_grpc_commands import Aborted, Context
from tests.test_grpc_evaluation_review import request


def fixture():
    value = request()
    value.ClearField("reviews")
    for i in range(35):
        value.reviews.add(candidate_id=f"candidate:{i}", decision="reject", reason="核" * 333)
    assert value.ByteSize() > 8192
    tx = SimpleNamespace(open=Mock(side_effect=AssertionError("No legacy root permitted")))
    dispatch = AsyncMock(
        return_value=EvaluationView(value.scope.run_id, 18, "awaiting_review", 0, "[]")
    )
    new_id = Mock(side_effect=uuid4)
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


async def test_full_original_review_rpc_batch_uses_first_authorized_clock_and_operation():
    f = fixture()
    before = datetime.now(UTC)
    response = await f.service.Review(f.value, Context())
    incoming, at = f.dispatch.await_args.args
    assert isinstance(incoming, ReviewStateInput)
    assert incoming.expected_version == 17 and len(incoming.values) == 35
    assert all(v.reviewer == incoming.scope.actor == "user:42" for v in incoming.values)
    assert all(v.reviewed_at == at for v in incoming.values)
    assert before <= at <= datetime.now(UTC)
    assert response.version == 18 and f.new_id.call_count == 1
    f.tx.open.assert_not_called()


@pytest.mark.parametrize("invalid", ["workload", "duplicate", "oversize"])
async def test_invalid_review_never_gets_operation_or_dispatch(invalid):
    f, context = fixture(), Context()
    if invalid == "workload":
        context.auth_context = lambda: {}
    elif invalid == "duplicate":
        f.value.reviews[1].candidate_id = f.value.reviews[0].candidate_id
    else:
        f.value.reviews.add(candidate_id="candidate:36", decision="reject", reason="核对")
    with pytest.raises(Aborted):
        await f.service.Review(f.value, context)
    f.new_id.assert_not_called()
    f.dispatch.assert_not_called()
    f.tx.open.assert_not_called()
