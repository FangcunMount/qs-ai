"""Frozen first serial dispatch and bounded handoff; no real Broker claim."""

import asyncio
import hashlib
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4, uuid5

import pytest
from reliable_messaging.transaction import LocalOutcome, Result

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.workflow_transport import worker_dispatch_sender as sender
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
    WorkerDispatchOperation,
    read_worker_dispatch_plan,
    worker_dispatch_intent,
)
from tests.test_evaluation_state_plan import fixture as state_fixture


@pytest.fixture
def frozen():
    f = state_fixture()
    owner = "worker:first"
    intent = worker_dispatch_intent(UUID(f.intent.run_id), 2, 7, owner)
    cp = ExecutionCheckpoint(
        "execution:first",
        "generation",
        "case:1",
        1,
        "",
        1,
        owner,
        "invocation:first",
        "prepared",
        f.facts.at,
        f.facts.at + timedelta(minutes=5),
    )
    digest = hashlib.sha256(b"first original source").digest()
    operation = WorkerDispatchOperation(cp, digest, digest, digest)
    facts = EvaluationStateFacts(intent, f.facts.at, 8, 2, operation.encode())
    visible = pb.EvaluationRuntimeState.FromString(f.state.SerializeToString())
    visible.version = 4
    message = prepare(
        pb.EVALUATION_STATE,
        str(uuid5(UUID(intent.run_id), "state:4")),
        intent.run_id,
        pb.MessagingBody(evaluation_state=visible),
        organization_id="7",
        original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        **f.keys,
    )
    plan = freeze_evaluation_state_plan(facts, str(uuid4()), message)
    return SimpleNamespace(**locals())


def test_first_checkpoint_and_source_have_canonical_bounded_immutable_codec(frozen):
    p = frozen
    assert WorkerDispatchOperation.decode(p.operation.encode()) == p.operation
    facts, op, archive = read_worker_dispatch_plan(p.intent, p.plan)
    assert facts == p.facts and op == p.operation and archive.wire == p.message.wire
    assert "execution:first" not in repr(op) and "source_digest" not in repr(op)
    assert worker_dispatch_intent(UUID(p.intent.run_id), 2, 7, p.owner) == p.intent
    assert worker_dispatch_intent(UUID(p.intent.run_id), 2, 7, "worker:other") == p.intent


@pytest.mark.parametrize(
    "damage", ["duplicate", "extra", "whitespace", "trailing", "magic", "oversize", "checkpoint"]
)
def test_bad_worker_facts_cannot_authorize_original_dispatch(frozen, damage):
    raw = frozen.operation.encode()
    if damage == "duplicate":
        raw = raw[:-1] + b',"source_digest":"' + b"a" * 64 + b'"}'
    elif damage == "extra":
        raw = raw[:-1] + b',"extra":1}'
    elif damage == "whitespace":
        raw = raw.replace(b'"checkpoint":', b'"checkpoint": ')
    elif damage == "trailing":
        raw += b"\n"
    elif damage == "magic":
        raw = raw.replace(b"/v1", b"/v2")
    elif damage == "oversize":
        raw += b"a" * 8192
    else:
        raw = raw.replace(b'"phase":"prepared"', b'"phase":"dispatching"')
    with pytest.raises(ValueError):
        WorkerDispatchOperation.decode(raw)


@pytest.mark.parametrize("value", [0, -1, True, 2**63 - 2, 2**63])
def test_two_original_cas_cannot_overflow_or_use_ambiguous_version(value):
    with pytest.raises(ValueError):
        worker_dispatch_intent(uuid4(), value, 7, "worker:first")


@pytest.mark.parametrize("owner", ["", " wrong", "x" * 129, None])
def test_worker_owner_is_bounded_before_protocol_lookup(owner):
    with pytest.raises(ValueError):
        worker_dispatch_intent(uuid4(), 2, 7, owner)


def rig(frozen):
    p = frozen
    tx, schemas = object(), object()
    planner = SimpleNamespace(
        transactions=tx,
        schemas=schemas,
        database_seconds=2,
        candidate=AsyncMock(
            return_value=(p.plan, SimpleNamespace(route=SimpleNamespace(provider="first")))
        ),
    )
    store = SimpleNamespace(lookup=AsyncMock(return_value=None))
    owned = SimpleNamespace(plan=p.plan)
    runtime = SimpleNamespace(
        store=store,
        reserve=AsyncMock(return_value=SimpleNamespace(acquired=True, execution=owned)),
        execute=AsyncMock(return_value=Result(LocalOutcome.UNKNOWN, ())),
    )
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    dispatch = sender.WorkerDispatchSender(
        tx, object(), runtime, schemas, p.owner, planner, capacity=pool
    )
    return SimpleNamespace(**locals())


async def test_existing_committed_or_unknown_dispatch_never_reacquires_model_or_replans(frozen):
    r = rig(frozen)
    r.store.lookup.return_value = object()
    with pytest.raises(sender.WorkerDispatchPending):
        await r.dispatch(UUID(frozen.intent.run_id), 2, 7)
    r.planner.candidate.assert_not_called()
    r.runtime.reserve.assert_not_called()
    r.runtime.execute.assert_not_called()
    r.pool.try_acquire("first", evaluation=True).release()


async def test_original_model_capacity_refusal_precedes_reservation_and_half_messages(frozen):
    r = rig(frozen)
    token = r.pool.try_acquire("first", evaluation=True)
    try:
        with pytest.raises(CheckpointConflict):
            await r.dispatch(UUID(frozen.intent.run_id), 2, 7)
        r.runtime.reserve.assert_not_called()
        r.runtime.execute.assert_not_called()
    finally:
        token.release()


@pytest.mark.parametrize("fault", ["reserve", "loser", "unknown", "cancel"])
async def test_incomplete_dispatch_releases_permit_without_model_retry_or_local_invocation(
    frozen, monkeypatch, fault
):
    r = rig(frozen)
    root = AsyncMock()
    monkeypatch.setattr(sender, "EvaluationStateRoot", lambda *a, **k: root)
    error = sender.WorkerDispatchPending
    if fault == "reserve":
        r.runtime.reserve.side_effect = ConnectionError("reservation unavailable")
        error = ConnectionError
    elif fault == "loser":
        r.runtime.reserve.return_value.acquired = False
    elif fault == "cancel":
        r.runtime.execute.side_effect = asyncio.CancelledError()
        error = asyncio.CancelledError
    with pytest.raises(error):
        await r.dispatch(UUID(frozen.intent.run_id), 2, 7)
    root.assert_not_called()
    token = r.pool.try_acquire("first", evaluation=True)
    assert token is not None
    token.release()
