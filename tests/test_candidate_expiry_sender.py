"""Candidate expiry freezes once, preserves Unknown and uses explicit SDK recovery gates."""

from contextlib import asynccontextmanager
from dataclasses import replace
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

from qs_ai.infrastructure.workflow_transport import candidate_expiry_sender as sender
from qs_ai.infrastructure.workflow_transport import candidate_expiry_state as state
from tests.route_fixtures import legacy_routes
from tests.test_candidate_expiry_state import fixture as planning
from tests.test_evaluation_serial_response_receipts import original as original


async def fixture(original, monkeypatch):
    p = planning(original, monkeypatch)
    projection = await state.project_candidate_expiry(
        p.db, p.request, p.cp.lease_expires_at, lock=False
    )
    project = AsyncMock(return_value=projection)
    monkeypatch.setattr(state, "project_candidate_expiry", project)
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield p.db
        finally:
            active = False

    seal = state.prepare

    def outside(*args, **kwargs):
        assert not active
        return seal(*args, **kwargs)

    prepare = Mock(side_effect=outside)
    monkeypatch.setattr(state, "prepare", prepare)
    p.db.connection = AsyncMock()
    previous = Mock()
    previous.mappings.return_value.one_or_none.return_value = {"sequence": 3, "version": 7}
    p.db.execute.return_value = previous
    tx = SimpleNamespace(open=opened)
    planner = state.CandidateExpiryPlanner(
        tx,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        route_resolver=legacy_routes(),
    )
    winner = await planner.candidate(p.request, p.cp.lease_expires_at)
    facts, first, archive = state.read_candidate_expiry_plan(
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
        retry=AsyncMock(return_value=reservation),
        replan=AsyncMock(return_value=reservation),
        execute=AsyncMock(return_value=Result(LocalOutcome.COMMITTED, ())),
        routing=legacy_routes(),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=archive))
    root = object()
    factory = Mock(return_value=root)
    monkeypatch.setattr(sender, "EvaluationStateRoot", factory)
    send = sender.CandidateExpirySender(
        tx,
        archives,
        runtime,
        signing_key=planner.signing_key,
        recipient_key=planner.recipient_key,
    )
    send._planner.candidate = AsyncMock(return_value=winner)
    return SimpleNamespace(**locals())


async def test_candidate_expiry_plan_keeps_coordinator_separate_from_original_claim(
    original, monkeypatch
):
    f = await fixture(original, monkeypatch)
    assert f.facts.source_version == 7 and f.facts.intent.expected_version == original.ref.version
    assert f.facts.sequence == 4 and f.first.claim == f.p.claim
    f.project.assert_awaited_once_with(f.p.db, f.p.request, f.p.cp.lease_expires_at, lock=False)
    f.prepare.assert_called_once()
    f.p.db.commit.assert_not_called()


@pytest.mark.parametrize("final_unknown", [False, True])
async def test_candidate_expiry_success_needs_exact_local_commit_and_archive(
    original, monkeypatch, final_unknown
):
    f = await fixture(original, monkeypatch)
    if final_unknown:
        f.runtime.execute.side_effect = TransactionSendError(
            "final_unknown", Result(LocalOutcome.COMMITTED, ())
        )
    assert await f.send(f.p.request, f.facts.at)
    f.runtime.reserve.assert_awaited_once_with(f.p.request.intent.request, f.winner)
    f.runtime.execute.assert_awaited_once_with(f.execution, f.root)
    f.runtime.retry.assert_not_called()
    f.runtime.replan.assert_not_called()


@pytest.mark.parametrize("status", [State.PREPARED, State.ABORTED, State.COMMITTED])
async def test_duplicate_never_renews_or_recreates_original_expired_work(
    original, monkeypatch, status
):
    f = await fixture(original, monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, status)
    if status == State.COMMITTED:
        assert await f.send(f.p.request, f.facts.at + timedelta(seconds=1))
    else:
        with pytest.raises(sender.CandidateExpiryPending):
            await f.send(f.p.request, f.facts.at + timedelta(seconds=1))
    f.send._planner.candidate.assert_not_called()
    f.runtime.execute.assert_not_called()
    f.runtime.retry.assert_not_called()
    f.runtime.replan.assert_not_called()


@pytest.mark.parametrize("damage", ["witness", "archive", "unknown"])
async def test_unproven_candidate_expiry_cannot_report_success_or_retry(
    original, monkeypatch, damage
):
    f = await fixture(original, monkeypatch)
    if damage == "witness":
        f.store.evidence.side_effect = None
        f.store.evidence.return_value = None
    elif damage == "archive":
        f.archives.read_committed.return_value = None
    else:
        f.runtime.execute.return_value = Result(LocalOutcome.UNKNOWN, ())
    with pytest.raises(sender.CandidateExpiryPending):
        await f.send(f.p.request, f.facts.at)
    assert f.runtime.execute.await_count == 1
    f.runtime.retry.assert_not_called()
    f.runtime.replan.assert_not_called()


async def test_explicit_retry_preserves_exact_first_frozen_expiry(original, monkeypatch):
    f = await fixture(original, monkeypatch)
    assert await f.send.retry(f.p.request, f.execution)
    assert f.runtime.retry.await_args.args[0].previous == f.execution
    f.runtime.reserve.assert_not_called()
    f.send._planner.candidate.assert_not_called()


async def test_explicit_replan_uses_new_coordinator_and_same_original_claim(original, monkeypatch):
    f = await fixture(original, monkeypatch)
    f.project.return_value = replace(f.projection, version=8)
    winner = await f.planner.candidate(f.p.request, f.facts.at + timedelta(seconds=1))
    facts, first, archive = state.read_candidate_expiry_plan(
        f.p.request, winner, route_resolver=legacy_routes()
    )
    owned = Execution(winner, 1, "m3-1", b"y" * 32, f.execution.lease_until)
    f.send._planner.candidate.return_value = winner
    f.runtime.replan.return_value = Reservation(owned, True, State.PREPARED)
    f.archives.read_committed.return_value = archive
    assert await f.send.replan(f.p.request, f.execution, facts.at)
    authorization = f.runtime.replan.await_args.args[0]
    assert authorization.previous == f.execution and authorization.candidate == winner
    assert facts.source_version == 8 and first.claim == f.first.claim
    assert archive.wire != f.archive.wire
    f.runtime.execute.assert_awaited_once_with(owned, f.root)
