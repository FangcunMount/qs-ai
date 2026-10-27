"""Correction replay keeps historical business and exact SDK evidence distinct."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.transaction import (
    Binding,
    BindingError,
    Evidence,
    Execution,
    LocalOutcome,
    Reservation,
    Result,
    State,
)

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.workflow_transport import evaluation_governance_dispatch as dispatch
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_correction_state_planner import fixture as planning


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    plan = await f.planner.candidate(f.operation, f.at)
    _, value = read_evaluation_state_plan(f.operation.intent, plan, route_resolver=legacy_routes())
    owned = Execution(plan, 1, "m3-1", b"x" * 32, f.at + timedelta(seconds=60))
    store = SimpleNamespace(
        binding=Binding("ai.mysql", "original", "m3-1"),
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=Reservation(owned, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        routing=legacy_routes(),
    )
    expected = object()
    root = SimpleNamespace(view=expected, business_error=None)
    factory = Mock(return_value=root)
    monkeypatch.setattr(dispatch, "CorrectionStateRoot", factory)
    classifier = AsyncMock(return_value=False)
    monkeypatch.setattr(dispatch, "input_conflicts", classifier)
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=value))
    send = dispatch.GovernanceStateDispatcher(
        f.tx,
        archives,
        runtime,
        signing_key=f.planner._signing_key,
        recipient_key=f.planner._recipient_key,
    )
    send._correction_planner.candidate = AsyncMock(return_value=plan)
    send._correction_planner.replay = AsyncMock(return_value=None)
    return SimpleNamespace(**locals())


async def test_new_correction_uses_one_original_root_and_its_own_response(monkeypatch):
    f = await fixture(monkeypatch)
    assert await f.send(f.f.operation, f.f.at) is f.expected
    f.runtime.execute.assert_awaited_once_with(f.owned, f.root)
    assert f.factory.call_args.args[3:5] == (f.f.operation, f.plan)
    f.send._correction_planner.candidate.assert_awaited_once_with(f.f.operation, f.f.at)


@pytest.mark.parametrize("protocol", [False, True])
async def test_original_replay_returns_current_business_view_without_new_halves(
    monkeypatch, protocol
):
    f = await fixture(monkeypatch)
    current = object()
    f.send._correction_planner.replay.return_value = current
    if protocol:
        f.store.lookup.return_value = Reservation(f.owned, False, State.COMMITTED)
    assert await f.send(f.f.operation, f.f.at) is current
    f.send._correction_planner.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.factory.assert_not_called()
    assert f.archives.read_committed.await_count == int(protocol)
    assert f.store.lookup.await_count == (1 if protocol else 2)


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_unresolved_sdk_record_never_adopts_a_historical_business_receipt(monkeypatch, state):
    f = await fixture(monkeypatch)
    f.store.lookup.return_value = Reservation(f.owned, False, state)
    f.send._correction_planner.replay.return_value = object()
    with pytest.raises(dispatch.GovernanceTransactionPending):
        await f.send(f.f.operation, f.f.at)
    f.runtime.execute.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.send._correction_planner.candidate.assert_not_called()


@pytest.mark.parametrize("damage", ["witness", "archive", "business"])
async def test_sdk_committed_replay_requires_both_exact_commit_and_original_business_receipt(
    monkeypatch, damage
):
    f = await fixture(monkeypatch)
    f.store.lookup.return_value = Reservation(f.owned, False, State.COMMITTED)
    f.send._correction_planner.replay.return_value = object()
    if damage == "witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "archive":
        f.archives.read_committed.return_value = None
    else:
        f.send._correction_planner.replay.return_value = None
    with pytest.raises(dispatch.GovernanceTransactionPending):
        await f.send(f.f.operation, f.f.at)
    f.runtime.execute.assert_not_called()


@pytest.mark.parametrize("where", ["candidate", "reservation"])
async def test_race_with_committed_same_command_reads_original_receipt_without_reexecution(
    monkeypatch, where
):
    f = await fixture(monkeypatch)
    current = object()
    f.send._correction_planner.replay.side_effect = [None, current]
    if where == "candidate":
        f.send._correction_planner.candidate.return_value = None
        f.store.lookup.side_effect = [None, Reservation(f.owned, False, State.COMMITTED)]
    else:
        f.runtime.reserve.return_value = Reservation(f.owned, False, State.COMMITTED)
    assert await f.send(f.f.operation, f.f.at) is current
    f.runtime.execute.assert_not_called()
    f.factory.assert_not_called()


@pytest.mark.parametrize("kind", ["input", "corrupt", "binding"])
async def test_input_conflict_needs_exact_primary_proof_and_never_exception_wording(
    monkeypatch, kind
):
    f = await fixture(monkeypatch)
    error = (
        BindingError("different key") if kind == "binding" else ValueError("arbitrary SDK wording")
    )
    f.store.lookup.side_effect = error
    f.classifier.return_value = kind == "input"
    with pytest.raises(CheckpointConflict if kind == "input" else type(error)) as caught:
        await f.send(f.f.operation, f.f.at)
    if kind != "input":
        assert caught.value is error
    assert f.classifier.await_count == int(kind != "binding")
    f.runtime.execute.assert_not_called()
    f.runtime.reserve.assert_not_called()
