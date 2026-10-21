"""The actual graph may invoke the model only after an explicit committed handoff."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import WorkerDispatchPending
from qs_ai.infrastructure.workflows import evaluation as graph
from tests.test_worker_dispatch_state import frozen as frozen


@pytest.mark.parametrize("fault", ["none", "pending", "finalization"])
async def test_graph_handoff_preserves_once_only_provider_call_and_finally_permit_release(
    monkeypatch, frozen, fault
):
    run_id = UUID(frozen.intent.run_id)
    at, cp = frozen.cp.claimed_at, frozen.cp
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    token = pool.try_acquire("first", evaluation=True)
    assert token is not None
    value = SimpleNamespace(
        capacity_token=token,
        claim=None,
        at=at,
        cp=cp,
        state=CheckpointState(run_id, 5, cp.mark_dispatching(cp.owner, at)),
        execution_id=cp.execution_id,
        messages=object(),
        route=object(),
        schema={},
        invocation_id="invocation:first",
    )
    old_prepare = AsyncMock(
        side_effect=AssertionError("Explicit dispatch cannot call legacy prepare")
    )
    prepare = AsyncMock(return_value=value)
    finish = AsyncMock(return_value=CheckpointState(run_id, 6, None))
    gateway = SimpleNamespace(generate_messages=AsyncMock(return_value=object()))
    monkeypatch.setattr(graph, "prepare_step", old_prepare)
    monkeypatch.setattr(graph, "finish_step", finish)
    if fault == "pending":
        token.release()  # An incomplete sender retains responsibility for its own permit cleanup.
        prepare.side_effect = WorkerDispatchPending("Existing dispatch needs original recovery")
    elif fault == "finalization":
        finish.side_effect = ConnectionError(
            "Finalization unavailable; model result must not be redone"
        )
    if fault == "none":
        result = await graph.execute_step(
            Mock(),
            run_id,
            3,
            1,
            "worker:first",
            gateway,
            Mock(),
            Mock(),
            clock=lambda: at,
            prepare_dispatch=prepare,
        )
        assert result.version == 6
    else:
        with pytest.raises(WorkerDispatchPending if fault == "pending" else ConnectionError):
            await graph.execute_step(
                Mock(),
                run_id,
                3,
                1,
                "worker:first",
                gateway,
                Mock(),
                Mock(),
                clock=lambda: at,
                prepare_dispatch=prepare,
            )
    prepare.assert_awaited_once_with(run_id, 3, 1)
    old_prepare.assert_not_called()
    assert gateway.generate_messages.await_count == (0 if fault == "pending" else 1)
    assert finish.await_count == (0 if fault == "pending" else 1)
    free = pool.try_acquire("first", evaluation=True)
    assert free is not None
    free.release()


async def test_serial_sender_cannot_be_used_as_a_candidate_claim_or_invoke_provider():
    prepare, gateway = AsyncMock(), SimpleNamespace(generate_messages=AsyncMock())
    with pytest.raises(ValueError, match="candidate"):
        await graph.execute_step(
            Mock(),
            uuid4(),
            3,
            1,
            "worker:first",
            gateway,
            Mock(),
            Mock(),
            candidate_limit=2,
            prepare_dispatch=prepare,
        )
    prepare.assert_not_called()
    gateway.generate_messages.assert_not_called()


@pytest.mark.parametrize("foreign", ["owner", "run", "version", "phase", "invocation"])
async def test_wrong_worker_handoff_never_invokes_model_and_returns_its_permit(
    monkeypatch, frozen, foreign
):
    cp, run_id = frozen.cp, UUID(frozen.intent.run_id)
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    token = pool.try_acquire("first", evaluation=True)
    value = SimpleNamespace(
        capacity_token=token,
        claim=None,
        cp=cp,
        at=cp.claimed_at,
        state=CheckpointState(run_id, 5, cp.mark_dispatching(cp.owner, cp.claimed_at)),
        execution_id=cp.execution_id,
        invocation_id=cp.invocation_id,
    )
    if foreign == "owner":
        value.cp = replace(cp, owner="worker:other")
    elif foreign == "run":
        value.state = replace(value.state, run_id=uuid4())
    elif foreign == "version":
        value.state = replace(value.state, version=4)
    elif foreign == "phase":
        value.cp = value.state.checkpoint
    else:
        value.invocation_id = "invocation:other"
    gateway = SimpleNamespace(generate_messages=AsyncMock())
    finish = AsyncMock()
    monkeypatch.setattr(graph, "finish_step", finish)
    with pytest.raises(CheckpointConflict):
        await graph.execute_step(
            Mock(),
            run_id,
            3,
            1,
            "worker:first",
            gateway,
            Mock(),
            Mock(),
            prepare_dispatch=AsyncMock(return_value=value),
        )
    gateway.generate_messages.assert_not_called()
    finish.assert_not_called()
    free = pool.try_acquire("first", evaluation=True)
    assert free is not None
    free.release()
