"""Original completion duplicates, explicit authority and unknown results stay separate."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.durable import MessageConflict
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import LocalOutcome, Result, State

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.infrastructure.workflow_transport import serial_semantic_sender as module
from tests.test_evaluation_serial_response_receipts import original as original
from tests.test_serial_semantic_state import frozen as frozen


def rig(p, monkeypatch):
    tx = object()
    planner = SimpleNamespace(
        transactions=tx,
        routes=object(),
        database_seconds=2,
        intent=AsyncMock(return_value=p.intent),
        candidate=AsyncMock(return_value=p.winner),
        verify=AsyncMock(),
    )
    store = SimpleNamespace(lookup=AsyncMock(return_value=None))
    execution = SimpleNamespace(plan=p.winner)
    acquired = SimpleNamespace(acquired=True, execution=execution, state=State.PREPARED)
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=acquired),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        retry=AsyncMock(return_value=acquired),
        replan=AsyncMock(return_value=acquired),
    )
    root = object()
    monkeypatch.setattr(module, "EvaluationStateRoot", lambda *a, **k: root)
    sender = module.SerialSemanticSender(tx, object(), runtime, planner)
    state = CheckpointState(p.p.ref.run_id, 7, None)
    proof = AsyncMock(return_value=state)
    monkeypatch.setattr(sender, "_committed", proof)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_existing_original_responsibility_never_plans_or_sends_again(
    frozen, monkeypatch, state
):
    p, r = frozen, rig(frozen, monkeypatch)
    r.store.lookup.return_value = SimpleNamespace(
        acquired=False, execution=r.execution, state=state
    )
    if state == State.COMMITTED:
        assert await r.sender(p.p.ref) == r.state
        r.proof.assert_awaited_once_with(p.intent, r.execution)
    else:
        with pytest.raises(module.SerialSemanticPending):
            await r.sender(p.p.ref)
        r.proof.assert_not_called()
    r.planner.candidate.assert_not_called()
    r.runtime.reserve.assert_not_called()
    r.runtime.execute.assert_not_called()
    r.runtime.retry.assert_not_called()
    r.runtime.replan.assert_not_called()


@pytest.mark.parametrize(
    "local",
    [LocalOutcome.COMMITTED, LocalOutcome.ABORTED, LocalOutcome.UNKNOWN, LocalOutcome.NOT_STARTED],
)
async def test_only_confirmed_original_commit_can_return_first_business_result(
    frozen, monkeypatch, local
):
    r = rig(frozen, monkeypatch)
    r.runtime.execute.return_value = Result(local, ())
    if local == LocalOutcome.COMMITTED:
        assert await r.sender(frozen.p.ref) == r.state
        r.proof.assert_awaited_once()
    else:
        with pytest.raises(module.SerialSemanticPending):
            await r.sender(frozen.p.ref)
        r.proof.assert_not_called()
    r.runtime.execute.assert_awaited_once_with(r.execution, r.root)


@pytest.mark.parametrize("local", [LocalOutcome.COMMITTED, LocalOutcome.UNKNOWN])
async def test_final_rpc_error_does_not_erase_local_committed_proof(frozen, monkeypatch, local):
    r = rig(frozen, monkeypatch)
    r.runtime.execute.side_effect = TransactionSendError(
        "isolated final RPC error", Result(local, ())
    )
    if local == LocalOutcome.COMMITTED:
        assert await r.sender(frozen.p.ref) == r.state
        r.proof.assert_awaited_once()
    else:
        with pytest.raises(TransactionSendError):
            await r.sender(frozen.p.ref)
        r.proof.assert_not_called()


async def test_exact_witness_or_receipt_unavailability_never_turns_into_business_reexecution(
    frozen, monkeypatch
):
    r = rig(frozen, monkeypatch)
    r.proof.side_effect = module.SerialSemanticPending("authority unavailable")
    with pytest.raises(module.SerialSemanticPending):
        await r.sender(frozen.p.ref)
    assert r.runtime.execute.await_count == 1
    r.runtime.retry.assert_not_called()
    r.runtime.replan.assert_not_called()


async def test_explicit_retry_verifies_readonly_first_source_before_public_sdk_authority(
    frozen, monkeypatch
):
    r = rig(frozen, monkeypatch)
    assert await r.sender.retry(frozen.p.ref, r.execution) == r.state
    r.planner.verify.assert_awaited_once_with(frozen.p.ref, frozen.winner)
    assert r.runtime.retry.await_args.args[0].previous is r.execution
    r.runtime.reserve.assert_not_called()
    r.runtime.replan.assert_not_called()


async def test_changed_source_rejects_retry_before_new_half_or_root(frozen, monkeypatch):
    r = rig(frozen, monkeypatch)
    r.planner.verify.side_effect = MessageConflict("source changed")
    with pytest.raises(MessageConflict):
        await r.sender.retry(frozen.p.ref, r.execution)
    r.runtime.retry.assert_not_called()
    r.runtime.execute.assert_not_called()


async def test_explicit_replan_preserves_original_first_candidate_and_uses_sdk_old_abort_cas(
    frozen, monkeypatch
):
    r = rig(frozen, monkeypatch)
    assert await r.sender.replan(frozen.p.ref, r.execution) == r.state
    r.planner.candidate.assert_awaited_once_with(
        frozen.p.ref, candidate_id=frozen.operation.candidate_id
    )
    authority = r.runtime.replan.await_args.args[0]
    assert authority.previous is r.execution and authority.candidate == frozen.winner
    r.runtime.reserve.assert_not_called()
    r.runtime.retry.assert_not_called()


@pytest.mark.parametrize("decision", ["retry", "replan"])
async def test_lost_explicit_execution_race_cannot_execute_the_original_root(
    frozen, monkeypatch, decision
):
    r = rig(frozen, monkeypatch)
    getattr(r.runtime, decision).return_value.acquired = False
    with pytest.raises(module.SerialSemanticPending):
        await getattr(r.sender, decision)(frozen.p.ref, r.execution)
    r.runtime.execute.assert_not_called()
