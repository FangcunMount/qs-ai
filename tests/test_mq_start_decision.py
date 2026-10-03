"""A durable pre-dispatch refusal is not transport acceptance or a mutable Session read."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from qs_ai.application.interpretation.ports import Receipt
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.workflow_transport.command_admission import _start_decision


def context():
    request = workflow.StartCommand(
        request_id=str(uuid4()), actor=workflow.Actor(org_id="1", subject_id="2"), testee_id="7"
    )
    receipt = Receipt(str(uuid4()), str(uuid4()), "blocked", 4)
    payload = {
        "session_id": receipt.session_id,
        "request_id": request.request_id,
        "version": receipt.version,
        "status": "blocked",
        "actor": {"org_id": "1", "subject_id": "2"},
        "failure_code": "configuration_unavailable",
    }
    tx, db = MagicMock(), AsyncMock()
    tx.open.return_value.__aenter__.return_value = db
    db.scalar.return_value = payload
    return tx, db, request, receipt, payload


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("configuration_unavailable", 9),
        ("admission_input_invalid", 3),
        ("admission_configuration_invalid", 9),
        ("participant_daily_capacity_exceeded", 8),
    ],
)
async def test_start_refusal_uses_only_original_persisted_event(code, status):
    tx, db, request, receipt, payload = context()
    payload["failure_code"] = code
    result = await _start_decision(tx, request, receipt, "1")
    assert (result.decision, result.code, result.grpc_status_code) == (pb.REJECTED, code, status)
    assert result.sequence == receipt.version and result.workflow_receipt.status == "blocked"
    assert result.workflow_receipt.session_id == receipt.session_id
    assert result.workflow_receipt.run_id == receipt.run_id
    statement = str(db.scalar.call_args.args[0])
    assert "result_outbox" in statement and "sessions" not in statement
    tx.commit.assert_not_called()
    db.commit.assert_not_awaited()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (None, None),
        ("failure_code", "provider_rate_limited"),
        ("status", "queued"),
        ("version", 5),
        ("session_id", "another-session"),
        ("request_id", "another-request"),
        ("actor", {"org_id": "2", "subject_id": "2"}),
    ],
)
async def test_missing_or_mismatched_evidence_is_technical_unknown(field, value):
    tx, db, request, receipt, payload = context()
    if field is None:
        db.scalar.return_value = value
    else:
        payload[field] = value
    with pytest.raises(RuntimeError, match="Original admission refusal evidence unavailable"):
        await _start_decision(tx, request, receipt, "1")


async def test_accepted_start_does_not_consult_current_state():
    tx, db, request, receipt, _ = context()
    receipt = Receipt(receipt.session_id, receipt.run_id, "queued", 2)
    result = await _start_decision(tx, request, receipt, "1")
    assert result.decision == pb.ACCEPTED and result.code == "" and result.grpc_status_code == 0
    tx.open.assert_not_called()
    db.scalar.assert_not_awaited()
