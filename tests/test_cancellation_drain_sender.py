"""SDK orchestration and worker seam; native root/Broker are separate gates."""

from contextlib import asynccontextmanager
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

from qs_ai.infrastructure.persistence.mysql import evaluation_worker as worker
from qs_ai.infrastructure.workflow_transport import cancellation_drain_sender as sender
from tests.route_fixtures import legacy_routes
from tests.test_cancellation_drain_state import fixture as planning


async def fixture(monkeypatch):
    p = planning(monkeypatch)
    operation = await p.planner.observe(p.scope, p.at)
    winner = await p.planner.candidate(operation, p.at)
    _, _, archive = sender.read_cancellation_drain_plan(
        operation, winner, route_resolver=legacy_routes()
    )
    execution = Execution(winner, 1, "m3-1", b"x" * 32, p.at + timedelta(seconds=60))
    store = SimpleNamespace(
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    owned = Reservation(execution, True, State.PREPARED)
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=owned),
        retry=AsyncMock(return_value=owned),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        routing=legacy_routes(),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=archive))
    root = object()
    factory = Mock(return_value=root)
    monkeypatch.setattr(sender, "EvaluationStateRoot", factory)
    send = sender.CancellationDrainSender(
        p.tx,
        archives,
        runtime,
        signing_key=p.planner.signing_key,
        recipient_key=p.planner.recipient_key,
    )
    send._planner.observe = AsyncMock(return_value=operation)
    send._planner.candidate = AsyncMock(return_value=winner)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("final_unknown", [False, True])
async def test_drain_requires_original_local_commit_even_when_final_reply_lost(
    monkeypatch, final_unknown
):
    f = await fixture(monkeypatch)
    if final_unknown:
        f.runtime.execute.side_effect = TransactionSendError(
            "final_unknown", Result(LocalOutcome.COMMITTED, ())
        )
    assert await f.send(f.p.scope, f.p.at)
    f.runtime.reserve.assert_awaited_once_with(f.operation.intent.request, f.winner)
    f.runtime.execute.assert_awaited_once_with(f.execution, f.root)
    f.runtime.retry.assert_not_called()
    assert f.factory.call_args.kwargs["isolation_level"] == "REPEATABLE READ"


@pytest.mark.parametrize("state", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_existing_drain_never_executes_again_or_reseals(monkeypatch, state):
    f = await fixture(monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, state)
    if state == State.COMMITTED:
        assert await f.send(f.p.scope, f.p.at)
    else:
        with pytest.raises(sender.CancellationDrainPending):
            await f.send(f.p.scope, f.p.at)
    f.runtime.execute.assert_not_called()
    f.runtime.retry.assert_not_called()
    f.send._planner.candidate.assert_not_called()


@pytest.mark.parametrize("damage", ["witness", "archive", "unknown"])
async def test_unproven_drain_never_becomes_success_or_new_cancel_request(monkeypatch, damage):
    f = await fixture(monkeypatch)
    if damage == "witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "archive":
        f.archives.read_committed.return_value = None
    else:
        f.runtime.execute.return_value = Result(LocalOutcome.UNKNOWN, ())
    with pytest.raises(sender.CancellationDrainPending):
        await f.send(f.p.scope, f.p.at)
    f.runtime.retry.assert_not_called()
    assert f.runtime.execute.await_count == 1


async def test_explicit_retry_uses_sdk_authorization_and_same_frozen_plan(monkeypatch):
    f = await fixture(monkeypatch)
    assert await f.send.retry(f.operation, f.execution)
    authorization = f.runtime.retry.await_args.args[0]
    assert authorization.previous == f.execution
    f.runtime.reserve.assert_not_called()
    f.send._planner.observe.assert_not_called()
    f.send._planner.candidate.assert_not_called()
    f.runtime.execute.assert_awaited_once_with(f.execution, f.root)


async def test_no_longer_eligible_drain_never_reserves_or_executes(monkeypatch):
    f = await fixture(monkeypatch)
    f.send._planner.observe.return_value = None
    assert not await f.send(f.p.scope, f.p.at)
    f.store.lookup.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_not_called()


@pytest.mark.parametrize("failure", [False, True])
async def test_worker_closes_hint_before_sender_and_never_commits_legacy_drain(
    monkeypatch, failure
):
    p = planning(monkeypatch)
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield p.db
        finally:
            active = False

    hint = Mock()
    hint.mappings.return_value.one_or_none.return_value = p.run
    p.db.execute.side_effect = None
    p.db.execute.return_value = hint
    legacy = AsyncMock(side_effect=AssertionError("No legacy root permitted"))
    monkeypatch.setattr(worker, "finish_cancellation", legacy)

    async def dispatch(scope, at):
        assert not active and scope == p.scope and at == p.at
        if failure:
            raise sender.CancellationDrainPending("Original responsibility remains")
        return True

    consumer = worker.EvaluationWorker(
        SimpleNamespace(open=opened),
        Mock(),
        Mock(),
        Mock(),
        "worker:original",
        clock=lambda: p.at,
        cancel_drain_dispatch=dispatch,
    )
    if failure:
        with pytest.raises(sender.CancellationDrainPending):
            await consumer._drain_cancellations()
    else:
        assert await consumer._drain_cancellations()
    assert p.db.execute.await_count == 1
    p.db.commit.assert_not_called()
    legacy.assert_not_called()
