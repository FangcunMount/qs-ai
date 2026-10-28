"""Governance orchestration only; actual roots/SDK/Broker remain separate gates."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Evidence,
    Execution,
    LocalOutcome,
    Reservation,
    Result,
    State,
)

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.workflow_transport import evaluation_governance_dispatch as dispatcher
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_governance_state_planner import fixture as planning


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    plan = await f.planner.candidate(f.operation, f.at)
    facts, value = read_evaluation_state_plan(
        f.operation.intent, plan, route_resolver=legacy_routes()
    )
    execution = Execution(plan, 1, "m3-1", b"x" * 32, f.at + timedelta(seconds=60))
    store = SimpleNamespace(
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    expected_view = object()
    root = SimpleNamespace(view=expected_view, business_error=None)
    root_factory = Mock(return_value=root)
    monkeypatch.setattr(dispatcher, "GovernanceStateRoot", root_factory)
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=Reservation(execution, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        routing=legacy_routes(),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=value))
    send = dispatcher.GovernanceStateDispatcher(
        f.tx,
        archives,
        runtime,
        signing_key=f.planner._signing_key,
        recipient_key=f.planner._recipient_key,
    )
    send._planner.candidate = AsyncMock(return_value=plan)
    return SimpleNamespace(**locals())


async def test_governance_returns_original_root_view_only_after_exact_fresh_proof(monkeypatch):
    f = await fixture(monkeypatch)
    assert await f.send(f.f.operation, f.f.at) is f.expected_view
    f.store.lookup.assert_awaited_once_with(f.f.operation.intent.request)
    f.send._planner.candidate.assert_awaited_once_with(f.f.operation, f.f.at)
    assert f.root_factory.call_args.args[3:5] == (f.f.operation, f.plan)
    f.runtime.execute.assert_awaited_once_with(f.execution, f.root)
    f.archives.read_committed.assert_awaited_once_with(f.value.envelope().message_id)


async def test_final_unknown_does_not_replace_local_commit_or_repeat_governance(monkeypatch):
    f = await fixture(monkeypatch)
    f.runtime.execute.side_effect = TransactionSendError(
        "final_outcome_unknown", Result(LocalOutcome.COMMITTED, ())
    )
    assert await f.send(f.f.operation, f.f.at) is f.expected_view
    assert f.runtime.execute.await_count == 1 and f.runtime.reserve.await_count == 1


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_existing_invocation_never_replans_or_substitutes_current_run_view(
    monkeypatch, state
):
    f = await fixture(monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, state)
    with pytest.raises(dispatcher.GovernanceTransactionPending):
        await f.send(f.f.operation, f.f.at)
    f.send._planner.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.root_factory.assert_not_called()


@pytest.mark.parametrize("damage", ["witness", "archive", "view"])
async def test_governance_claim_needs_exact_witness_archive_and_original_response(
    monkeypatch, damage
):
    f = await fixture(monkeypatch)
    if damage == "witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "archive":
        f.archives.read_committed.return_value = None
    else:
        f.root.view = None
    with pytest.raises(dispatcher.GovernanceTransactionPending):
        await f.send(f.f.operation, f.f.at)
    assert f.runtime.execute.await_count == 1


@pytest.mark.parametrize("structured", [False, True])
async def test_proved_abort_preserves_original_cas_error_without_business_retry(
    monkeypatch, structured
):
    f = await fixture(monkeypatch)
    original = CheckpointConflict("Original current version changed")
    f.root.business_error = original
    result = Result(LocalOutcome.ABORTED, ())
    if structured:
        f.runtime.execute.side_effect = TransactionSendError("final_unknown", result)
    else:
        f.runtime.execute.return_value = result
    with pytest.raises(CheckpointConflict) as caught:
        await f.send(f.f.operation, f.f.at)
    assert caught.value is original and f.runtime.execute.await_count == 1
    f.store.evidence.assert_not_called()


@pytest.mark.parametrize("outcome", [LocalOutcome.NOT_STARTED, LocalOutcome.UNKNOWN])
async def test_business_error_without_proved_abort_is_never_recast_as_conflict(
    monkeypatch, outcome
):
    f = await fixture(monkeypatch)
    f.root.business_error = CheckpointConflict("Unproved business error")
    error = TransactionSendError("unknown", Result(outcome, ()))
    f.runtime.execute.side_effect = error
    with pytest.raises(TransactionSendError) as caught:
        await f.send(f.f.operation, f.f.at)
    assert caught.value is error
    f.store.evidence.assert_not_called()
