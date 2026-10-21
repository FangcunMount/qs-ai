"""Local ownership/failure contracts; mocks do not prove SQL atomicity or Broker recovery."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import BindingError, Evidence, Execution, LocalOutcome, State

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport.admission_receipts import AdmissionDecision
from qs_ai.infrastructure.workflow_transport.transaction_admission import AdmissionRoot
from qs_ai.infrastructure.workflow_transport.transaction_command import freeze_admission_plan
from tests.test_admission_plan import fixture


def root_fixture(decision=pb.ACCEPTED, *, with_events=True, isolation_level=None):
    incoming, receipt, state = fixture(decision)
    publications = ((receipt, 2),) if decision == pb.HELD else ((receipt, 2), (state, 2))
    plan = freeze_admission_plan(incoming, str(uuid4()), publications)
    execution = Execution(plan, 1, "m3-1", b"x" * 32, datetime.now(UTC) + timedelta(seconds=60))
    events = []

    def event(name):
        events.append(name)

    original_root = SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        get_transaction=Mock(return_value=original_root),
        info={},
        begin=AsyncMock(side_effect=lambda: event("begin")),
        connection=AsyncMock(side_effect=lambda **_: event("connection")),
        commit=AsyncMock(side_effect=lambda: event("commit")),
        rollback=AsyncMock(side_effect=lambda: event("rollback")),
    )

    @asynccontextmanager
    async def opened():
        events.append("open")
        try:
            yield db
        finally:
            events.append("close")

    guard = SimpleNamespace(
        put=AsyncMock(side_effect=lambda _: event("archive")),
        commit_witness=AsyncMock(side_effect=lambda: event("witness")),
    )

    async def bind(original, candidate):
        assert original is db and candidate is execution
        events.append("guard")
        return guard

    archives = SimpleNamespace(bind=AsyncMock(side_effect=bind))
    store = SimpleNamespace(
        abort=AsyncMock(return_value=True), evidence=AsyncMock(return_value=None)
    )
    inbox = SimpleNamespace(
        reserve_command=AsyncMock(return_value=None), decide_transaction=AsyncMock()
    )
    expected = pb.MessagingBody.FromString(receipt.body).command_receipt
    result = AdmissionDecision(
        incoming.organization_id, 2, decision, workflow_receipt=expected.workflow_receipt
    )

    async def business(original, envelope, body):
        assert original is db and envelope.message_id == incoming.envelope().message_id
        assert body == incoming.body_message()
        events.append("business")
        return result

    action = AsyncMock(side_effect=business)
    recorder = SimpleNamespace(
        plan_digest=plan.digest, flush=AsyncMock(side_effect=lambda _: event("state-proof"))
    )
    local = AdmissionRoot(
        SimpleNamespace(open=opened),
        archives,
        store,
        inbox,
        incoming,
        reservation_token=str(uuid4()),
        business=action,
        events=recorder if with_events else None,
        isolation_level=isolation_level,
    )
    return SimpleNamespace(**locals())


async def test_original_root_guard_precedes_business_and_all_archives_witness_precede_commit():
    f = root_fixture()
    assert await f.local(f.execution) == LocalOutcome.COMMITTED
    assert f.events == [
        "open",
        "begin",
        "guard",
        "business",
        "state-proof",
        "archive",
        "archive",
        "witness",
        "commit",
        "close",
    ]
    f.inbox.reserve_command.assert_awaited_once()
    f.inbox.decide_transaction.assert_awaited_once()
    f.store.abort.assert_not_called()
    with pytest.raises(BindingError):
        await f.local(f.execution)
    assert f.action.await_count == 1


async def test_held_receipt_never_invokes_business():
    f = root_fixture(pb.HELD)
    assert await f.local(f.execution) == LocalOutcome.COMMITTED
    f.action.assert_not_called()
    assert f.events.count("archive") == 1


async def test_changed_business_result_rolls_back_before_archiving_and_requires_durable_abort():
    f = root_fixture()
    f.result.workflow_receipt.status = "changed"
    assert await f.local(f.execution) == LocalOutcome.ABORTED
    f.guard.put.assert_not_called()
    f.guard.commit_witness.assert_not_called()
    f.db.commit.assert_not_called()
    f.db.rollback.assert_awaited_once()
    f.store.abort.assert_awaited_once_with(f.execution)


@pytest.mark.parametrize("abort_proof", [False, True])
async def test_rollback_is_not_broker_rollback_proof_without_persisted_abort(abort_proof):
    f = root_fixture()
    f.action.side_effect = RuntimeError("technical business failure")
    f.store.abort.return_value = abort_proof
    assert await f.local(f.execution) == (
        LocalOutcome.ABORTED if abort_proof else LocalOutcome.UNKNOWN
    )
    f.db.rollback.assert_awaited_once()
    f.db.commit.assert_not_called()


async def test_commit_reply_loss_is_unknown_without_fresh_exact_witness_and_never_aborts():
    f = root_fixture()
    f.db.commit.side_effect = ConnectionError("reply unavailable")
    assert await f.local(f.execution) == LocalOutcome.UNKNOWN
    assert f.store.evidence.await_count == len(f.execution.references)
    f.store.abort.assert_not_called()
    assert f.action.await_count == 1


async def test_legacy_inbox_decision_cannot_be_adopted_as_new_witness():
    f = root_fixture()
    f.inbox.reserve_command.return_value = {"decision": "accepted", "receipt_id": str(uuid4())}
    assert await f.local(f.execution) == LocalOutcome.ABORTED
    f.action.assert_not_called()
    f.guard.put.assert_not_called()
    f.db.commit.assert_not_called()


async def test_failed_rollback_is_unknown_and_cannot_authorize_abort():
    f = root_fixture()
    f.action.side_effect = RuntimeError("technical business failure")
    f.db.rollback.side_effect = ConnectionError("rollback unavailable")
    assert await f.local(f.execution) == LocalOutcome.UNKNOWN
    f.store.abort.assert_not_called()


async def test_business_cannot_replace_or_commit_the_original_root_and_claim_abort():
    f = root_fixture()

    async def replaced(*args):
        f.db.get_transaction.return_value = None
        return f.result

    f.action.side_effect = replaced
    assert await f.local(f.execution) == LocalOutcome.UNKNOWN
    f.guard.put.assert_not_called()
    f.store.abort.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("committed", [False, True])
async def test_commit_reply_loss_is_resolved_only_by_exact_fresh_execution_and_all_branches(
    committed,
):
    f = root_fixture()
    f.db.commit.side_effect = ConnectionError("reply unavailable")

    async def proof(reference):
        assert f.events[-1] == "close", "authoritative read follows root cleanup"
        if committed:
            return Evidence(reference, State.COMMITTED, State.COMMITTED)
        return None

    f.store.evidence.side_effect = proof
    assert await f.local(f.execution) == (
        LocalOutcome.COMMITTED if committed else LocalOutcome.UNKNOWN
    )
    f.store.abort.assert_not_called()
    assert f.action.await_count == 1


async def test_cancellation_is_not_abort_proof_without_completed_rollback_and_durable_abort():
    import asyncio

    f = root_fixture()
    f.action.side_effect = asyncio.CancelledError()
    f.store.abort.return_value = False
    assert await f.local(f.execution) == LocalOutcome.UNKNOWN
    f.db.rollback.assert_awaited_once()
    f.guard.commit_witness.assert_not_called()
    f.db.commit.assert_not_called()


async def test_zero_capability_cannot_open_the_business_root():
    from dataclasses import replace

    f = root_fixture()
    with pytest.raises(BindingError):
        await f.local(replace(f.execution, token=bytes(32)))
    assert not f.events
    f.action.assert_not_called()


@pytest.mark.parametrize("evidence_failure", ["unavailable", "mixed", "foreign"])
async def test_commit_reply_loss_never_trusts_partial_foreign_or_unavailable_evidence(
    evidence_failure,
):
    from dataclasses import replace

    f = root_fixture()
    f.db.commit.side_effect = ConnectionError("reply unavailable")
    first = f.execution.references[0]

    async def proof(reference):
        if evidence_failure == "unavailable":
            raise ConnectionError("primary unavailable")
        if evidence_failure == "foreign":
            return Evidence(
                replace(reference, fence=reference.fence + 1), State.COMMITTED, State.COMMITTED
            )
        return Evidence(
            reference, State.COMMITTED, State.COMMITTED if reference == first else State.PREPARED
        )

    f.store.evidence.side_effect = proof
    assert await f.local(f.execution) == LocalOutcome.UNKNOWN
    f.store.abort.assert_not_called()
    assert f.action.await_count == 1


async def test_state_plan_without_original_transcription_is_refused_before_business_root():
    f = root_fixture(with_events=False)
    with pytest.raises(BindingError):
        await f.local(f.execution)
    assert not f.events
    f.action.assert_not_called()


async def test_state_transcription_cannot_belong_to_another_winner():
    f = root_fixture()
    f.recorder.plan_digest = bytes(32)
    with pytest.raises(BindingError):
        await f.local(f.execution)
    assert not f.events


async def test_no_state_plan_cannot_run_legacy_message_commit_hook():
    f = root_fixture(pb.HELD, with_events=False)
    legacy = SimpleNamespace(flush=AsyncMock())
    f.db.info["state_events"] = legacy
    assert await f.local(f.execution) == LocalOutcome.ABORTED
    legacy.flush.assert_not_called()
    f.db.commit.assert_not_called()
    f.action.assert_not_called()


async def test_current_read_mode_is_chosen_before_guard_and_preserves_original_root():
    f = root_fixture(isolation_level="READ COMMITTED")
    assert await f.local(f.execution) == LocalOutcome.COMMITTED
    assert f.events[:5] == ["open", "begin", "connection", "guard", "business"]
    f.db.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "READ COMMITTED"}
    )
    assert f.db.get_transaction() is f.original_root


@pytest.mark.parametrize("invalid", ["AUTOCOMMIT", "SERIALIZABLE", True])
def test_admission_root_cannot_silently_choose_other_transaction_modes(invalid):
    with pytest.raises(ValueError):
        root_fixture(isolation_level=invalid)
