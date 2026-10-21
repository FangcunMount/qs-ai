"""Evaluation planning and the original borrowed root share explicit authority."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.contracts.workflow import messaging_pb2 as messaging
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow.command_mapping import evaluation_arguments
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission


@pytest.mark.parametrize("cancel", [False, True])
def test_original_scope_version_reason_and_explicit_discard_are_preserved(cancel):
    cls = pb.EvaluationCancelCommand if cancel else pb.EvaluationStartCommand
    value = cls(
        scope=pb.EvaluationQuery(run_id=str(uuid4()), organization_id=7, operator_user_id=9),
        expected_version=4,
        reason=" original reason ",
        confirm=True,
    )
    if cancel:
        value.discard = False
    scope, version, reason, discard = evaluation_arguments(value)
    assert str(scope.run_id) == value.scope.run_id and scope.actor == "user:9"
    assert (version, reason, discard) == (4, " original reason ", False if cancel else None)


@pytest.mark.parametrize("damage", ["version", "confirm", "run", "discard", "oversize"])
def test_original_refusal_boundaries_remain_explicit(damage):
    value = pb.EvaluationCancelCommand(
        scope=pb.EvaluationQuery(run_id=str(uuid4()), organization_id=7, operator_user_id=9),
        expected_version=1,
        reason="cancel",
        confirm=True,
        discard=False,
    )
    if damage == "version":
        value.expected_version = 0
    elif damage == "confirm":
        value.confirm = False
    elif damage == "run":
        value.scope.run_id = "invalid"
    elif damage == "discard":
        value.ClearField("discard")
    else:
        value.reason = "x" * 8192
    with pytest.raises(ValueError):
        evaluation_arguments(value)


@pytest.mark.parametrize("cancel", [False, True])
async def test_frozen_evaluation_factory_borrows_original_root_and_explicit_clock(cancel):
    store = SimpleNamespace(start=AsyncMock(), cancel=AsyncMock())
    factory = Mock(return_value=store)
    at = datetime(2026, 10, 21, tzinfo=UTC)
    admission = WorkflowCommandAdmission(
        *(SimpleNamespace() for _ in range(6)),
        evaluation_factory=factory,
        clock=lambda: at,
    )
    body = messaging.MessagingBody()
    request = body.evaluation_cancel if cancel else body.evaluation_start
    request.scope.CopyFrom(
        pb.EvaluationQuery(run_id=str(uuid4()), organization_id=7, operator_user_id=9)
    )
    request.expected_version, request.reason, request.confirm = 1, "explicit", True
    if cancel:
        request.discard = False
    method = store.cancel if cancel else store.start
    method.return_value = EvaluationView(
        request.scope.run_id, 2, "canceled" if cancel else "collecting", 0, "[]"
    )

    @asynccontextmanager
    async def savepoint():
        yield

    root = SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        info={}, in_transaction=lambda: True, begin_nested=savepoint, get_transaction=lambda: root
    )
    decision = await admission.admit(
        db,
        messaging.MessagingEnvelope(
            kind=messaging.EVALUATION_CANCEL if cancel else messaging.EVALUATION_START
        ),
        body,
    )
    assert factory.call_args.args[0].session is db
    args = evaluation_arguments(request)
    kwargs = {"confirm": True, **({"discard": False} if cancel else {})}
    method.assert_awaited_once_with(*args[:3], at, **kwargs)
    assert decision.decision == messaging.ACCEPTED and decision.evaluation_receipt.version == 2
