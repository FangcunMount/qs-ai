"""Receiver orchestration only; native SDK/transport proof remains a separate required gate."""

import base64
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from jwcrypto import jwk
from reliable_messaging.transaction import (
    Evidence,
    Execution,
    LocalOutcome,
    Reservation,
    Result,
    State,
)
from reliable_messaging.wire import decode

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.solution_models import DEFAULT_EDITABLE_MODELS
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.hold_plan import read_hold_token
from qs_ai.infrastructure.workflow_transport.messaging import CHANNELS, COMMANDS
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    freeze_admission_plan,
    read_admission_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_receiver import (
    COMMAND_TOPIC,
    CommandTransactionPending,
    TransactionCommandReceiver,
)
from tests.test_admission_plan import fixture as messages


def fixture():
    incoming, receipt, state = messages()
    plan = freeze_admission_plan(incoming, str(uuid4()), ((receipt, 2), (state, 2)))
    execution = Execution(plan, 1, "m3-1", b"x" * 32, datetime.now(UTC) + timedelta(seconds=60))
    db = SimpleNamespace(begin=AsyncMock(), commit=AsyncMock())

    @asynccontextmanager
    async def opened():
        yield db

    base = WorkflowCommandAdmission(
        SimpleNamespace(),
        ParticipantCapacityPolicy(),
        EvaluationCapacityPolicy(),
        DEFAULT_EDITABLE_MODELS,
        None,
        EvaluationRuntimeLimits(),
    )
    store = SimpleNamespace(
        read_command=AsyncMock(return_value=None),
        logical_attempts=AsyncMock(return_value=0),
        technical_failure=AsyncMock(),
    )
    boundary = SimpleNamespace(
        transactions=SimpleNamespace(open=opened),
        store=store,
        admission=base,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        qs_recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        resolve_command=AsyncMock(
            return_value=(incoming.envelope(), incoming.body, incoming.body_message())
        ),
        isolate=AsyncMock(),
    )
    sdk_store = SimpleNamespace(lookup=AsyncMock(return_value=None), evidence=AsyncMock())
    runtime = SimpleNamespace(
        store=sdk_store,
        reserve=AsyncMock(return_value=Reservation(execution, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
    )
    archives = SimpleNamespace(read_committed=AsyncMock())
    receiver = TransactionCommandReceiver(boundary, base, runtime, archives)
    candidate = Mock(candidate=AsyncMock(return_value=plan))
    receiver._planners = {pb.START: candidate}
    received = SimpleNamespace(
        wire=incoming.wire,
        provider="rocketmq",
        logical_topic=COMMANDS,
        physical_topic=COMMAND_TOPIC,
    )
    return SimpleNamespace(**locals())


async def test_lookup_precedes_planning_and_executor_uses_persisted_reservation_winner():
    from dataclasses import replace

    f = fixture()
    winner = replace(f.plan, operation_id=str(uuid4()))
    owned = Execution(winner, 1, "m3-1", b"y" * 32, f.execution.lease_until)
    f.runtime.reserve.return_value = Reservation(owned, True, State.PREPARED)
    local = AsyncMock()
    f.receiver._root = Mock(return_value=local)
    f.receiver._committed = AsyncMock()
    await f.receiver.receive_command(f.received)
    f.sdk_store.lookup.assert_awaited_once_with(f.incoming.request)
    f.candidate.candidate.assert_awaited_once_with(f.incoming)
    f.receiver._root.assert_called_once_with(f.incoming, winner)
    f.runtime.execute.assert_awaited_once_with(owned, local)
    f.receiver._committed.assert_awaited_once_with(f.incoming, owned)
    f.store.technical_failure.assert_not_called()


async def test_already_committed_sdk_request_checks_authority_without_replanning_or_sending():
    f = fixture()
    f.sdk_store.lookup.return_value = Reservation(f.execution, False, State.COMMITTED)
    f.receiver._committed = AsyncMock()
    await f.receiver.receive_command(f.received)
    f.receiver._committed.assert_awaited_once_with(f.incoming, f.execution)
    f.candidate.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()


async def test_legacy_committed_inbox_is_not_adopted_as_transaction_witness():
    f = fixture()
    f.store.read_command.return_value = {"decision": "accepted", "receipt_id": str(uuid4())}
    await f.receiver.receive_command(f.received)
    f.candidate.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.sdk_store.evidence.assert_not_called()


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_replay_cannot_automatically_restart_or_replan_existing_execution(state):
    f = fixture()
    f.sdk_store.lookup.return_value = Reservation(f.execution, False, state)
    with pytest.raises(CommandTransactionPending):
        await f.receiver.receive_command(f.received)
    f.candidate.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.store.technical_failure.assert_awaited_once()


@pytest.mark.parametrize(
    "outcome", [LocalOutcome.UNKNOWN, LocalOutcome.ABORTED, LocalOutcome.NOT_STARTED]
)
async def test_no_fin_when_original_business_is_not_known_committed(outcome):
    f = fixture()
    f.receiver._root = Mock(return_value=AsyncMock())
    f.runtime.execute.return_value = Result(outcome, ())
    f.receiver._committed = AsyncMock()
    with pytest.raises(CommandTransactionPending):
        await f.receiver.receive_command(f.received)
    f.receiver._committed.assert_not_called()
    f.runtime.execute.assert_awaited_once()


@pytest.mark.parametrize("damage", ["witness", "archive", "inbox", "none"])
async def test_duplicate_fin_requires_exact_witness_archive_and_original_inbox(damage):
    f = fixture()
    f.sdk_store.evidence.side_effect = [
        Evidence(ref, State.COMMITTED, State.COMMITTED) if damage != "witness" else None
        for ref in f.execution.references
    ]
    values = read_admission_plan(f.incoming, f.plan)
    f.archives.read_committed.side_effect = [
        None if damage == "archive" else value for value in values
    ]
    f.store.read_command.return_value = {
        "receipt_id": "wrong" if damage == "inbox" else f.receipt.envelope.message_id
    }
    if damage == "none":
        await f.receiver._committed(f.incoming, f.execution)
    else:
        with pytest.raises(CommandTransactionPending):
            await f.receiver._committed(f.incoming, f.execution)
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["provider", "logical_topic", "physical_topic"])
async def test_wrong_actual_transport_binding_cannot_reach_auth_or_sdk(damage):
    f = fixture()
    setattr(f.received, damage, "wrong")
    await f.receiver.receive_command(f.received)
    f.boundary.isolate.assert_awaited_once_with(f.incoming.wire, "transaction_route_mismatch")
    f.boundary.resolve_command.assert_not_called()
    f.sdk_store.lookup.assert_not_called()


async def test_only_local_budget_can_freeze_one_held_receipt_and_no_business_state():
    f = fixture()
    f.store.logical_attempts.return_value = 8
    f.receiver._root = Mock(return_value=AsyncMock())
    f.receiver._committed = AsyncMock()
    await f.receiver.receive_command(f.received)
    candidate = f.runtime.reserve.await_args.args[1]
    assert len(candidate.branches) == 1 and len(read_hold_token(f.incoming, candidate)) == 36
    f.candidate.candidate.assert_not_called()


@pytest.mark.parametrize("provider", ["rocketmq", "nsq"])
async def test_physical_failure_only_archives_and_never_authorizes_business_or_hold(provider):
    f = fixture()
    envelope = decode(f.incoming.wire)
    wrapper = json.dumps(
        {
            "type": "reliable-messaging.failed.v1",
            "provider": provider,
            "topic": COMMANDS,
            "channel": CHANNELS[COMMANDS],
            "physical_topic": COMMAND_TOPIC,
            "uuid": envelope.message_id,
            "metadata": envelope.metadata,
            "payload": base64.b64encode(envelope.payload).decode(),
            "attempts": 2**63,
        }
    ).encode()
    await f.receiver.failed_command(wrapper)
    f.boundary.isolate.assert_awaited_once_with(
        wrapper, "handler_failed" if provider == "rocketmq" else "invalid_failure_wire"
    )
    f.sdk_store.lookup.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.store.logical_attempts.assert_not_called()


async def test_local_technical_budget_read_begins_its_original_transaction_before_binding():
    f = fixture()
    started = False

    async def begin():
        nonlocal started
        started = True

    async def logical_attempts(db, envelope):
        assert started, "local ledger requires its own original active transaction"
        assert db is f.db and envelope == f.incoming.envelope()
        return 0

    f.db.begin.side_effect = begin
    f.store.logical_attempts.side_effect = logical_attempts
    f.receiver._root = Mock(return_value=AsyncMock())
    f.receiver._committed = AsyncMock()
    await f.receiver.receive_command(f.received)
    f.db.begin.assert_awaited_once()
    f.db.commit.assert_not_called()


async def test_sdk_final_unknown_error_keeps_committed_business_after_exact_authority_check():
    from reliable_messaging.rocketmq_transaction import TransactionSendError

    f = fixture()
    f.receiver._root = Mock(return_value=AsyncMock())
    f.receiver._committed = AsyncMock()
    f.runtime.execute.side_effect = TransactionSendError(
        "final_outcome_unknown", Result(LocalOutcome.COMMITTED, ())
    )
    await f.receiver.receive_command(f.received)
    f.receiver._committed.assert_awaited_once_with(f.incoming, f.execution)
    f.store.technical_failure.assert_not_called()


@pytest.mark.parametrize(
    "outcome", [LocalOutcome.UNKNOWN, LocalOutcome.ABORTED, LocalOutcome.NOT_STARTED]
)
async def test_sdk_error_without_committed_business_never_acknowledges_or_restarts(outcome):
    from reliable_messaging.rocketmq_transaction import TransactionSendError

    f = fixture()
    f.receiver._root = Mock(return_value=AsyncMock())
    f.receiver._committed = AsyncMock()
    error = TransactionSendError("unresolved", Result(outcome, ()))
    f.runtime.execute.side_effect = error
    with pytest.raises(TransactionSendError) as caught:
        await f.receiver.receive_command(f.received)
    assert caught.value is error
    f.receiver._committed.assert_not_called()
    f.runtime.execute.assert_awaited_once()


async def test_even_sdk_committed_error_cannot_ack_when_application_evidence_is_missing():
    from reliable_messaging.rocketmq_transaction import TransactionSendError

    f = fixture()
    f.receiver._root = Mock(return_value=AsyncMock())
    f.receiver._committed = AsyncMock(side_effect=CommandTransactionPending("exact proof missing"))
    f.runtime.execute.side_effect = TransactionSendError(
        "final_outcome_unknown", Result(LocalOutcome.COMMITTED, ())
    )
    with pytest.raises(CommandTransactionPending):
        await f.receiver.receive_command(f.received)
    f.store.technical_failure.assert_awaited_once()
