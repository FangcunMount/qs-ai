"""Original command refusal mapping stays shared with pre-half planning."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.capacity import CapacityExceeded
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.ports import AccessDenied, NotFound
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.messaging import parse_body
from tests.test_transaction_command import command_fixture


def fixture(error, kind=pb.START):
    envelope, raw, _ = command_fixture(kind)
    root = SimpleNamespace(is_active=True)

    @asynccontextmanager
    async def nested():
        yield

    db = SimpleNamespace(
        info={"evaluation_events": {"original"}},
        get_transaction=Mock(return_value=root),
        in_transaction=Mock(return_value=True),
        begin_nested=nested,
    )
    admission = WorkflowCommandAdmission(*(SimpleNamespace() for _ in range(6)))
    admission._apply = AsyncMock(side_effect=error)
    return admission, db, envelope, parse_body(envelope, raw)


@pytest.mark.parametrize(
    ("error", "kind", "code", "status"),
    [
        (AccessDenied(), pb.START, "resource_access_denied", 7),
        (NotFound(), pb.START, "resource_not_found", 5),
        (CapacityExceeded(), pb.START, "evaluation_capacity_exceeded", 8),
        (CheckpointConflict(), pb.START, "evaluation_state_conflict", 10),
        (
            RuleViolation("participant_daily_capacity_exceeded"),
            pb.START,
            "participant_daily_capacity_exceeded",
            8,
        ),
        (RuleViolation("version_conflict"), pb.START, "version_conflict", 10),
        (RuleViolation("invalid_state"), pb.START, "invalid_state", 10),
        (RuleViolation("invalid_input"), pb.START, "invalid_input", 3),
        (ValueError("invalid"), pb.START, "invalid_command", 3),
        (RuleViolation("invalid_input"), pb.EVALUATION_START, "invalid_input", 10),
    ],
    ids=[
        "access",
        "missing",
        "capacity",
        "checkpoint",
        "daily",
        "version",
        "state",
        "input",
        "value",
        "evaluation",
    ],
)
async def test_original_known_refusal_codes_keep_status_and_root_ownership(
    error, kind, code, status
):
    admission, db, envelope, body = fixture(error, kind)
    decision = await admission.admit(db, envelope, body)
    assert decision.decision == pb.REJECTED and decision.sequence == 1
    assert decision.code == code and decision.grpc_status_code == status
    assert decision.organization_id == "7"
    assert decision.workflow_receipt is None and decision.evaluation_receipt is None
    assert db.info["evaluation_events"] == {"original"}
    admission._apply.assert_awaited_once()


@pytest.mark.parametrize(
    "error", [ConnectionError("database"), RuntimeError("unknown")], ids=["database", "unknown"]
)
async def test_technical_failure_never_becomes_a_command_refusal(error):
    admission, db, envelope, body = fixture(error)
    with pytest.raises(type(error)):
        await admission.admit(db, envelope, body)
    assert db.info["evaluation_events"] == {"original"}


async def test_start_borrows_frozen_service_on_original_root_without_committing():
    from qs_ai.application.interpretation.ports import Receipt
    from qs_ai.infrastructure.persistence.mysql.database import Transactions

    _, db, envelope, body = fixture(RuntimeError())
    # The original normal constructor remains valid; only the transaction caller
    # supplies a service assembled from its durable winner's frozen resources.
    result = Receipt("session", "run", "queued", 2)
    service = SimpleNamespace(start_external=AsyncMock(return_value=result))
    factory = Mock(return_value=service)
    admission = WorkflowCommandAdmission(
        *(SimpleNamespace() for _ in range(6)), interpretation_factory=factory
    )
    original = Transactions.borrowed(db)
    decision = await admission._apply(original, envelope, body, "7")
    factory.assert_called_once_with(original)
    assert decision.workflow_receipt.session_id == result.session_id
    assert db.get_transaction() is original.transaction
