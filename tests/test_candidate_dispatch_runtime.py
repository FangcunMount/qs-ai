"""Candidate handoff, shared capacity and first dispatch remain separate from model retries."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4, uuid5

import pytest
from reliable_messaging.transaction import LocalOutcome, Result

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.workflow_transport import candidate_dispatch_sender as sender
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
    CandidateDispatchOperation,
    candidate_dispatch_intent,
    read_candidate_dispatch_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflows import evaluation as graph
from tests.test_worker_dispatch_state import frozen as frozen


@pytest.fixture
def candidate(frozen):
    p = frozen
    run_id = UUID(p.intent.run_id)
    intent = candidate_dispatch_intent(run_id, 2, 7)
    operation = CandidateDispatchOperation(p.cp, 3, b"s" * 32, b"i" * 32, b"r" * 32)
    facts = EvaluationStateFacts(intent, p.facts.at, 8, 2, operation.encode())
    visible = pb.EvaluationRuntimeState.FromString(p.visible.SerializeToString())
    visible.version = 3
    message = prepare(
        pb.EVALUATION_STATE,
        str(uuid5(run_id, "state:3")),
        str(run_id),
        pb.MessagingBody(evaluation_state=visible),
        organization_id="7",
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        **p.f.keys,
    )
    winner = freeze_evaluation_state_plan(facts, str(uuid4()), message)
    return SimpleNamespace(**locals())


def test_candidate_dispatch_has_separate_bounded_codec_and_one_coordinator_cas(candidate):
    p = candidate
    assert CandidateDispatchOperation.decode(p.operation.encode()) == p.operation
    assert read_candidate_dispatch_plan(p.intent, p.winner)[:2] == (p.facts, p.operation)
    for limit in (0, True, 33):
        with pytest.raises(ValueError):
            replace(p.operation, limit=limit)
    for raw in (
        p.operation.encode() + b" ",
        p.operation.encode().replace(b"/v1", b"/v2"),
        p.operation.encode()[:-1] + b',"limit":3}',
    ):
        with pytest.raises(ValueError):
            CandidateDispatchOperation.decode(raw)
    assert "execution:first" not in repr(p.operation)


@pytest.mark.parametrize("version", [True, 0, -1, 2**63 - 1])
def test_candidate_dispatch_version_cannot_overflow_single_cas(version):
    with pytest.raises(ValueError):
        candidate_dispatch_intent(uuid4(), version, 7)


def rig(candidate):
    p = candidate
    tx = Mock()
    planner = SimpleNamespace(
        transactions=tx,
        database_seconds=2,
        clock=lambda: p.p.cp.claimed_at,
        candidate=AsyncMock(
            return_value=(p.winner, SimpleNamespace(route=SimpleNamespace(provider="first")))
        ),
    )
    store = SimpleNamespace(lookup=AsyncMock(return_value=None))
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(
            return_value=SimpleNamespace(acquired=True, execution=SimpleNamespace(plan=p.winner))
        ),
        execute=AsyncMock(return_value=Result(LocalOutcome.UNKNOWN, ())),
    )
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    dispatch = sender.CandidateDispatchSender(
        tx, Mock(), runtime, p.p.owner, planner, capacity=pool
    )
    return SimpleNamespace(**locals())


async def test_existing_candidate_dispatch_never_reacquires_capacity_or_invokes_model(candidate):
    r = rig(candidate)
    r.store.lookup.return_value = object()
    with pytest.raises(sender.CandidateDispatchPending):
        await r.dispatch(candidate.run_id, 2, 7)
    r.planner.candidate.assert_not_called()
    r.runtime.reserve.assert_not_called()
    r.runtime.execute.assert_not_called()


@pytest.mark.parametrize("failure", ["reserve", "loser", "unknown", "cancel"])
async def test_unconfirmed_candidate_dispatch_returns_original_model_capacity(candidate, failure):
    r = rig(candidate)
    error = sender.CandidateDispatchPending
    if failure == "reserve":
        error = ConnectionError
        r.runtime.reserve.side_effect = error("Reservation unavailable")
    elif failure == "loser":
        r.runtime.reserve.return_value.acquired = False
    elif failure == "cancel":
        error = asyncio.CancelledError
        r.runtime.execute.side_effect = error()
    with pytest.raises(error):
        await r.dispatch(candidate.run_id, 2, 7)
    token = r.pool.try_acquire("first", evaluation=True)
    assert token is not None
    token.release()


async def test_saturated_original_capacity_refuses_before_half_message_reservation(candidate):
    r = rig(candidate)
    token = r.pool.try_acquire("first", evaluation=True)
    try:
        with pytest.raises(CheckpointConflict):
            await r.dispatch(candidate.run_id, 2, 7)
        r.runtime.reserve.assert_not_called()
        r.runtime.execute.assert_not_called()
    finally:
        token.release()


@pytest.mark.parametrize("fault", ["none", "pending", "completion", "wrong_handoff"])
async def test_actual_candidate_graph_calls_provider_once_and_uses_first_durable_result(
    monkeypatch, candidate, fault
):
    p = candidate
    cp = p.p.cp.mark_dispatching(p.p.owner, p.p.cp.claimed_at)
    claim = SlotClaim(p.run_id, 3, cp)
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    token = pool.try_acquire("first", evaluation=True)
    prepared = SimpleNamespace(
        cp=cp,
        claim=claim,
        state=CheckpointState(p.run_id, 3, cp),
        at=cp.dispatch_started_at,
        execution_id=cp.execution_id,
        invocation_id=cp.invocation_id,
        capacity_token=token,
        messages=Mock(),
        route=Mock(),
        schema={},
    )
    db = SimpleNamespace(commit=AsyncMock())

    @asynccontextmanager
    async def opened():
        yield db

    tx = SimpleNamespace(open=opened)
    runtime = SimpleNamespace(
        transactions=tx,
        limit=3,
        prepare=AsyncMock(return_value=prepared),
        complete=AsyncMock(return_value=CheckpointState(p.run_id, 4, None)),
    )
    gateway = SimpleNamespace(generate_messages=AsyncMock(return_value=object()))
    save, old = AsyncMock(), AsyncMock()
    monkeypatch.setattr(graph, "save_response", save)
    monkeypatch.setattr(graph, "prepare_step", old)
    monkeypatch.setattr(graph, "finish_step", old)
    if fault == "pending":
        token.release()
        runtime.prepare.side_effect = sender.CandidateDispatchPending("Original pending dispatch")
    elif fault == "completion":
        runtime.complete.side_effect = ConnectionError("Finalize first durable result later")
    elif fault == "wrong_handoff":
        prepared.claim = replace(claim, version=4)
    kwargs = dict(clock=lambda: cp.claimed_at, candidate_limit=3, candidate_runtime=runtime)
    if fault == "none":
        result = await graph.execute_step(
            tx, p.run_id, 2, 7, p.p.owner, gateway, Mock(), Mock(), **kwargs
        )
        assert result.version == 4
        runtime.complete.assert_awaited_once()
    else:
        error = {
            "pending": sender.CandidateDispatchPending,
            "completion": ConnectionError,
            "wrong_handoff": CheckpointConflict,
        }[fault]
        with pytest.raises(error):
            await graph.execute_step(
                tx, p.run_id, 2, 7, p.p.owner, gateway, Mock(), Mock(), **kwargs
            )
    assert gateway.generate_messages.await_count == (
        0 if fault in ("pending", "wrong_handoff") else 1
    )
    assert save.await_count == gateway.generate_messages.await_count
    assert db.commit.await_count == save.await_count
    old.assert_not_called()
    token = pool.try_acquire("first", evaluation=True)
    assert token is not None
    token.release()
