"""Worker orchestration contracts; actual driver, MySQL and Broker have separate gates."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

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

from qs_ai.infrastructure.workflow_transport.evaluation_preflight_dispatch import (
    PreflightStateDispatcher,
    PreflightTransactionPending,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_preflight_state_planner import fixture as planning


async def fixture(monkeypatch):
    f = planning(monkeypatch)
    run_id = f.f.run_id
    plan = await f.planner.candidate(f.intent)
    facts, value = read_evaluation_state_plan(f.intent, plan, route_resolver=legacy_routes())
    execution = Execution(plan, 1, "m3-1", b"x" * 32, datetime.now(UTC) + timedelta(seconds=60))
    store = SimpleNamespace(
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=Reservation(execution, True, State.PREPARED)),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        routing=legacy_routes(),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=value))
    dispatcher = PreflightStateDispatcher(
        f.planner._transactions,
        archives,
        runtime,
        signing_key=f.planner._signing_key,
        recipient_key=f.planner._recipient_key,
    )
    candidate = AsyncMock(return_value=plan)
    dispatcher._planner.candidate = candidate
    return SimpleNamespace(**{**vars(f), **locals()})


async def test_worker_precheck_precedes_plan_and_local_executor_uses_durable_winner(monkeypatch):
    f = await fixture(monkeypatch)
    winner = replace(f.plan, operation_id=str(uuid4()))
    owned = replace(f.execution, plan=winner)
    f.runtime.reserve.return_value = Reservation(owned, True, State.PREPARED)
    await f.dispatcher(f.run_id, 2, 7)
    f.store.lookup.assert_awaited_once_with(f.intent.request)
    f.candidate.assert_awaited_once_with(f.intent)
    f.runtime.reserve.assert_awaited_once_with(f.intent.request, f.plan)
    actual, local = f.runtime.execute.await_args.args
    assert actual is owned and local._plan_digest == winner.digest
    assert local._facts == f.facts
    f.archives.read_committed.assert_awaited_once_with(f.value.envelope().message_id)


async def test_repeated_committed_hint_is_proved_before_reading_current_changed_business(
    monkeypatch,
):
    f = await fixture(monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, State.COMMITTED)
    await f.dispatcher(f.run_id, 2, 7)
    f.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.store.evidence.assert_awaited_once()


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED])
async def test_existing_unresolved_or_aborted_operation_never_replans_or_invokes_business(
    monkeypatch, state
):
    f = await fixture(monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, state)
    with pytest.raises(PreflightTransactionPending):
        await f.dispatcher(f.run_id, 2, 7)
    f.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()


async def test_local_committed_structured_final_unknown_still_requires_exact_authority(monkeypatch):
    f = await fixture(monkeypatch)
    f.runtime.execute.side_effect = TransactionSendError(
        "final_outcome_unknown", Result(LocalOutcome.COMMITTED, ())
    )
    await f.dispatcher(f.run_id, 2, 7)
    f.store.evidence.assert_awaited_once()
    f.archives.read_committed.assert_awaited_once()


@pytest.mark.parametrize(
    "outcome", [LocalOutcome.NOT_STARTED, LocalOutcome.ABORTED, LocalOutcome.UNKNOWN]
)
async def test_half_or_local_failure_does_not_become_worker_success(monkeypatch, outcome):
    f = await fixture(monkeypatch)
    error = TransactionSendError("unresolved", Result(outcome, ()))
    f.runtime.execute.side_effect = error
    with pytest.raises(TransactionSendError) as caught:
        await f.dispatcher(f.run_id, 2, 7)
    assert caught.value is error
    f.store.evidence.assert_not_called()
    f.archives.read_committed.assert_not_called()


@pytest.mark.parametrize("damage", ["missing-witness", "foreign-witness", "missing-archive"])
async def test_claimed_local_commit_cannot_succeed_with_incomplete_or_foreign_original_proof(
    monkeypatch, damage
):
    f = await fixture(monkeypatch)
    if damage == "missing-witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "foreign-witness":
        f.store.evidence.side_effect = lambda ref: Evidence(
            replace(ref, fence=ref.fence + 1), State.COMMITTED, State.COMMITTED
        )
    else:
        f.archives.read_committed.return_value = None
    with pytest.raises(PreflightTransactionPending):
        await f.dispatcher(f.run_id, 2, 7)
    assert f.runtime.execute.await_count == 1
