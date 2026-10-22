"""Uncommitted dispatch recovery uses public bounded Replan and original shared capacity."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from reliable_messaging.transaction import Execution, Reservation, State

from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
    CandidateDispatchPending,
    CandidateDispatchRecovery,
)
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
    WorkerDispatchPending,
    WorkerDispatchRecovery,
)
from tests.test_candidate_dispatch_runtime import candidate as candidate
from tests.test_candidate_dispatch_runtime import rig as candidate_rig
from tests.test_worker_dispatch_state import frozen as frozen
from tests.test_worker_dispatch_state import rig as worker_rig


@pytest.fixture(params=["serial", "candidate"])
def rig(request, frozen, candidate):
    p = candidate if request.param == "candidate" else frozen
    r = candidate_rig(candidate) if request.param == "candidate" else worker_rig(frozen)
    intent = p.intent
    plan = p.winner if request.param == "candidate" else p.plan
    before = Execution(plan, 1, "m3-1", b"x" * 32, p.facts.at + timedelta(seconds=60))
    new_plan = replace(plan, operation_id=str(uuid4()))
    owned = Execution(new_plan, 1, "m3-1", b"y" * 32, before.lease_until)
    r.store.lookup.return_value = Reservation(before, False, State.ABORTED)
    r.planner.candidate.return_value = (
        new_plan,
        SimpleNamespace(route=SimpleNamespace(provider="first")),
    )
    r.runtime.replan = AsyncMock(return_value=Reservation(owned, True, State.PREPARED))
    owner = (
        CandidateDispatchRecovery(r.dispatch)
        if request.param == "candidate"
        else WorkerDispatchRecovery(r.dispatch)
    )
    pending = CandidateDispatchPending if request.param == "candidate" else WorkerDispatchPending
    return SimpleNamespace(**locals())


async def test_ordinary_duplicate_stays_strict_even_with_old_abort(rig):
    p = rig
    with pytest.raises(p.pending):
        await p.r.dispatch(UUID(p.intent.run_id), 2, 7)
    p.r.planner.candidate.assert_not_called()
    p.r.runtime.replan.assert_not_called()
    p.r.runtime.execute.assert_not_called()


@pytest.mark.parametrize("state", [State.PREPARED, State.COMMITTED])
async def test_explicit_replan_cannot_recover_unproved_or_committed_dispatch(rig, state):
    p = rig
    p.r.store.lookup.return_value = Reservation(p.before, False, state)
    with pytest.raises(p.pending):
        await p.r.dispatch.replan(UUID(p.intent.run_id), 2, 7, p.before)
    p.r.planner.candidate.assert_not_called()
    p.r.runtime.replan.assert_not_called()


async def test_exact_abort_replan_consumes_original_capacity_and_never_bypasses_sdk(rig):
    p = rig

    async def bounded(authorization):
        assert p.r.pool.try_acquire("first", evaluation=True) is None
        assert authorization.previous == p.before and authorization.candidate == p.new_plan
        return Reservation(p.owned, True, State.PREPARED)

    p.r.runtime.replan.side_effect = bounded
    with pytest.raises(p.pending):  # Controlled runtime leaves the new local result Unknown.
        await p.r.dispatch.replan(UUID(p.intent.run_id), 2, 7, p.before)
    p.r.runtime.reserve.assert_not_called()
    assert p.r.runtime.replan.await_count == p.r.runtime.execute.await_count == 1
    token = p.r.pool.try_acquire("first", evaluation=True)
    assert token is not None
    token.release()


@pytest.mark.parametrize("state", [State.PREPARED, State.COMMITTED, State.ABORTED])
async def test_only_explicit_host_owner_selects_replan_and_never_promotes_commit(rig, state):
    p = rig
    p.r.store.lookup.return_value = Reservation(p.before, False, state)
    p.r.dispatch.replan = AsyncMock(return_value=object())
    if state == State.ABORTED:
        await p.owner(UUID(p.intent.run_id), 2, 7)
        p.r.dispatch.replan.assert_awaited_once_with(UUID(p.intent.run_id), 2, 7, p.before)
    else:
        with pytest.raises(p.pending):
            await p.owner(UUID(p.intent.run_id), 2, 7)
        p.r.dispatch.replan.assert_not_called()


async def test_changed_old_fence_cannot_authorize_replan_or_take_capacity(rig):
    p = rig
    foreign = replace(p.before, fence=p.before.fence + 1)
    with pytest.raises(p.pending):
        await p.r.dispatch.replan(UUID(p.intent.run_id), 2, 7, foreign)
    p.r.planner.candidate.assert_not_called()
    p.r.runtime.replan.assert_not_called()
