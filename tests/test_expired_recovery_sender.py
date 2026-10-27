"""Expiry freeze and SDK ordering; original database and Broker gates remain separate."""

from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from jwcrypto import jwk
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Evidence,
    Execution,
    LocalOutcome,
    Reservation,
    Result,
    State,
)

from qs_ai.infrastructure.workflow_transport import expired_recovery_sender as sender
from qs_ai.infrastructure.workflow_transport import expired_recovery_state as state
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_expired_recovery_source import source as source
from tests.test_expired_recovery_state import setup


async def fixture(source, monkeypatch):
    p = setup(source, monkeypatch)
    projection = await state.project_expired_recovery(
        source.db, p.request, "worker:first", p.cp.lease_expires_at, lock=False
    )
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield source.db
        finally:
            active = False

    project = AsyncMock(return_value=projection)
    monkeypatch.setattr(state, "project_expired_recovery", project)
    seal = state.prepare

    def outside(*args, **kwargs):
        assert not active
        return seal(*args, **kwargs)

    prepare = Mock(side_effect=outside)
    monkeypatch.setattr(state, "prepare", prepare)
    source.db.connection = AsyncMock()
    previous = Mock()
    previous.mappings.return_value.one_or_none.return_value = {"sequence": 3, "version": 7}
    source.db.execute.reset_mock()
    source.db.execute.side_effect = None
    source.db.execute.return_value = previous
    tx = SimpleNamespace(open=opened)
    planner = state.ExpiredRecoveryPlanner(
        tx,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        route_resolver=legacy_routes(),
    )
    winner = await planner.candidate(p.request, "worker:first", p.cp.lease_expires_at)
    facts, actor, archive = state.read_expired_recovery_plan(
        p.request, winner, route_resolver=legacy_routes()
    )
    execution = Execution(
        winner, 1, "m3-1", b"x" * 32, p.cp.lease_expires_at + timedelta(seconds=60)
    )
    store = SimpleNamespace(
        lookup=AsyncMock(return_value=None),
        evidence=AsyncMock(side_effect=lambda ref: Evidence(ref, State.COMMITTED, State.COMMITTED)),
    )
    reservation = Reservation(execution, True, State.PREPARED)
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=reservation),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        retry=AsyncMock(return_value=reservation),
        routing=legacy_routes(),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=archive))
    root = object()
    factory = Mock(return_value=root)
    monkeypatch.setattr(sender, "EvaluationStateRoot", factory)
    send = sender.ExpiredRecoverySender(
        tx,
        archives,
        runtime,
        signing_key=planner.signing_key,
        recipient_key=planner.recipient_key,
    )
    send._planner.candidate = AsyncMock(return_value=winner)
    return SimpleNamespace(**locals())


async def test_expiry_planner_freezes_first_actor_time_projection_without_business_write(
    source, monkeypatch
):
    f = await fixture(source, monkeypatch)
    assert f.facts.intent.expected_version == f.p.request.version
    assert f.facts.source_version is None
    assert f.actor == "worker:first" and f.facts.at == f.p.cp.lease_expires_at
    assert f.facts.sequence == 4 and f.facts.intent == f.p.request.intent
    f.project.assert_awaited_once_with(
        source.db, f.p.request, "worker:first", f.facts.at, lock=False
    )
    source.db.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in source.db.execute.await_args_list
    )
    source.db.commit.assert_not_called()
    f.prepare.assert_called_once()


@pytest.mark.parametrize("final_unknown", [False, True])
async def test_sender_prepares_through_sdk_then_requires_exact_local_commit(
    source, monkeypatch, final_unknown
):
    f = await fixture(source, monkeypatch)
    if final_unknown:
        f.runtime.execute.side_effect = TransactionSendError(
            "final_unknown", Result(LocalOutcome.COMMITTED, ())
        )
    assert await f.send(f.p.request, "worker:first", f.facts.at)
    f.runtime.reserve.assert_awaited_once_with(f.p.request.intent.request, f.winner)
    f.runtime.execute.assert_awaited_once_with(f.execution, f.root)
    assert f.factory.call_args.args[4] is f.winner
    f.runtime.retry.assert_not_called()


@pytest.mark.parametrize("status", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_new_instance_reads_original_execution_without_new_wire_or_model_authority(
    source, monkeypatch, status
):
    f = await fixture(source, monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, status)
    if status == State.COMMITTED:
        assert await f.send(f.p.request, "worker:replacement", f.facts.at + timedelta(seconds=1))
    else:
        with pytest.raises(sender.ExpiredRecoveryPending):
            await f.send(f.p.request, "worker:replacement", f.facts.at + timedelta(seconds=1))
    f.send._planner.candidate.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.runtime.retry.assert_not_called()


@pytest.mark.parametrize("damage", ["witness", "archive", "unknown"])
async def test_expiry_sender_never_confuses_unproved_result_with_rollback_or_success(
    source, monkeypatch, damage
):
    f = await fixture(source, monkeypatch)
    if damage == "witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "archive":
        f.archives.read_committed.return_value = None
    else:
        f.runtime.execute.return_value = Result(LocalOutcome.UNKNOWN, ())
    with pytest.raises(sender.ExpiredRecoveryPending):
        await f.send(f.p.request, "worker:first", f.facts.at)
    assert f.runtime.execute.await_count == 1
    f.runtime.retry.assert_not_called()


async def test_explicit_expiry_retry_preserves_first_plan_and_uses_public_sdk_gate(
    source, monkeypatch
):
    f = await fixture(source, monkeypatch)
    assert await f.send.retry(f.p.request, f.execution)
    assert f.runtime.retry.await_args.args[0].previous == f.execution
    f.send._planner.candidate.assert_not_called()
    f.runtime.reserve.assert_not_called()
    f.runtime.execute.assert_awaited_once_with(f.execution, f.root)
