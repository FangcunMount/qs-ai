"""Original blocked START outcomes require precise immutable business evidence."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.interpretation.ports import Receipt
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.command_admission import _start_decision


def fixture(code="configuration_unavailable"):
    request = workflow.StartCommand(
        request_id=str(uuid4()), actor=workflow.Actor(org_id="7", subject_id="9")
    )
    receipt = Receipt(str(uuid4()), str(uuid4()), "blocked", 4)
    event = {
        "session_id": receipt.session_id,
        "request_id": request.request_id,
        "version": 4,
        "status": "blocked",
        "actor": {"org_id": "7", "subject_id": "9"},
        "failure_code": code,
    }
    db = SimpleNamespace(scalar=AsyncMock(return_value=event))

    @asynccontextmanager
    async def opened():
        yield db

    tx = SimpleNamespace(open=opened)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("configuration_unavailable", 9),
        ("admission_configuration_invalid", 9),
        ("admission_input_invalid", 3),
        ("participant_daily_capacity_exceeded", 8),
    ],
)
async def test_original_start_refusal_keeps_exact_business_code_and_status(code, status):
    f = fixture(code)
    decision = await _start_decision(f.tx, f.request, f.receipt, "7")
    assert decision.decision == pb.REJECTED and decision.grpc_status_code == status
    assert decision.code == code and decision.sequence == 4
    assert decision.workflow_receipt.session_id == f.receipt.session_id
    f.db.scalar.assert_awaited_once()


@pytest.mark.parametrize("wrong", ["missing", "request", "session", "actor", "version", "code"])
async def test_missing_foreign_or_corrupt_original_refusal_is_technical_unknown(wrong):
    f = fixture()
    if wrong == "missing":
        f.db.scalar.return_value = None
    elif wrong == "request":
        f.event["request_id"] = str(uuid4())
    elif wrong == "session":
        f.event["session_id"] = str(uuid4())
    elif wrong == "actor":
        f.event["actor"] = {"org_id": "8", "subject_id": "9"}
    elif wrong == "version":
        f.event["version"] = 5
    else:
        f.event["failure_code"] = "unknown"
    with pytest.raises(RuntimeError, match="Original admission refusal evidence unavailable"):
        await _start_decision(f.tx, f.request, f.receipt, "7")


async def test_accepted_original_start_never_reads_an_unrelated_refusal_record():
    f = fixture()
    tx = SimpleNamespace(open=Mock(side_effect=AssertionError("no refusal read")))
    receipt = Receipt(f.receipt.session_id, f.receipt.run_id, "queued", 2)
    decision = await _start_decision(tx, f.request, receipt, "7")
    assert decision.decision == pb.ACCEPTED and decision.sequence == 2
    assert not decision.code and decision.grpc_status_code == 0
