"""Complete serial transaction graph cannot drop back to legacy terminal writes."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import WorkerDispatchPending
from qs_ai.infrastructure.workflows import evaluation as graph
from tests.route_fixtures import legacy_routes
from tests.test_worker_dispatch_state import frozen as frozen


@pytest.mark.parametrize("fault", ["none", "pending", "response", "terminal", "cancel"])
async def test_paired_serial_graph_saves_first_response_before_terminal_without_legacy_fallback(
    frozen, monkeypatch, fault
):
    cp, run_id = frozen.cp, UUID(frozen.intent.run_id)
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    token = pool.try_acquire("first", evaluation=True)
    prepared = SimpleNamespace(
        claim=None,
        cp=cp,
        state=CheckpointState(run_id, 5, cp.mark_dispatching(cp.owner, cp.claimed_at)),
        at=cp.claimed_at,
        invocation_id=cp.invocation_id,
        execution_id=cp.execution_id,
        capacity_token=token,
        messages=object(),
        route=object(),
        schema={},
    )
    tx = Mock()
    stages = []
    terminal_entered = asyncio.Event()

    async def save(*args):
        stages.append("response")
        if fault == "response":
            raise ConnectionError("Durable first response unavailable")

    async def complete(*args):
        assert stages == ["response"]
        stages.append("terminal")
        if fault == "terminal":
            raise ConnectionError("Original terminal responsibility remains")
        if fault == "cancel":
            terminal_entered.set()
            await asyncio.Future()
        return CheckpointState(run_id, 6, None)

    runtime = SimpleNamespace(
        transactions=tx,
        prepare=AsyncMock(return_value=prepared),
        save_response=AsyncMock(side_effect=save),
        complete=AsyncMock(side_effect=complete),
        routing=legacy_routes(),
    )
    old = AsyncMock(side_effect=AssertionError("Paired serial runtime cannot use legacy writes"))
    monkeypatch.setattr(graph, "prepare_step", old)
    monkeypatch.setattr(graph, "finish_step", old)
    gateway = SimpleNamespace(generate_messages=AsyncMock(return_value=object()))
    if fault == "pending":
        token.release()  # Sender owns cleanup until an actual committed handoff.
        runtime.prepare.side_effect = WorkerDispatchPending("Original dispatch pending")

    async def call():
        return await graph.execute_step(
            tx,
            run_id,
            3,
            7,
            cp.owner,
            gateway,
            Mock(),
            Mock(),
            clock=lambda: cp.claimed_at,
            serial_runtime=runtime,
        )

    if fault == "none":
        assert (await call()).version == 6
    elif fault == "cancel":
        task = asyncio.create_task(call())
        try:
            await asyncio.wait_for(terminal_entered.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    else:
        error = {
            "pending": WorkerDispatchPending,
            "response": ConnectionError,
            "terminal": ConnectionError,
            "cancel": asyncio.CancelledError,
        }[fault]
        with pytest.raises(error):
            await call()
    assert gateway.generate_messages.await_count == (0 if fault == "pending" else 1)
    assert runtime.save_response.await_count == (0 if fault == "pending" else 1)
    assert runtime.complete.await_count == (0 if fault in ("pending", "response") else 1)
    old.assert_not_called()
    available = pool.try_acquire("first", evaluation=True)
    assert available is not None
    available.release()


@pytest.mark.parametrize("foreign", ["host", "candidate", "partial_dispatch"])
async def test_serial_runtime_rejects_foreign_or_partial_composition_before_any_model(foreign):
    tx = Mock()
    runtime = SimpleNamespace(
        transactions=Mock() if foreign == "host" else tx,
        prepare=AsyncMock(),
        routing=legacy_routes(),
    )
    gateway = SimpleNamespace(generate_messages=AsyncMock())
    options = {"candidate_limit": 3} if foreign == "candidate" else {}
    if foreign == "partial_dispatch":
        options["prepare_dispatch"] = AsyncMock()
    with pytest.raises(ValueError, match="[Ss]erial"):
        await graph.execute_step(
            tx,
            Mock(),
            3,
            7,
            "worker:first",
            gateway,
            Mock(),
            Mock(),
            serial_runtime=runtime,
            **options,
        )
    runtime.prepare.assert_not_called()
    gateway.generate_messages.assert_not_called()


@pytest.mark.parametrize("foreign", ["run", "version", "owner", "phase", "invocation"])
async def test_paired_serial_graph_retains_original_handoff_validation_and_permit_cleanup(
    frozen, monkeypatch, foreign
):
    cp, run_id = frozen.cp, UUID(frozen.intent.run_id)
    pool = ModelCapacity({"first": ProviderCapacity(2, 1)}, 1)
    token = pool.try_acquire("first", evaluation=True)
    prepared = SimpleNamespace(
        claim=None,
        cp=cp,
        at=cp.claimed_at,
        state=CheckpointState(run_id, 5, cp.mark_dispatching(cp.owner, cp.claimed_at)),
        execution_id=cp.execution_id,
        invocation_id=cp.invocation_id,
        capacity_token=token,
    )
    if foreign == "run":
        prepared.state = replace(prepared.state, run_id=UUID(int=1))
    elif foreign == "version":
        prepared.state = replace(prepared.state, version=4)
    elif foreign == "owner":
        prepared.cp = replace(cp, owner="worker:other")
    elif foreign == "phase":
        prepared.cp = prepared.state.checkpoint
    else:
        prepared.invocation_id = "invocation:other"
    tx = Mock()
    runtime = SimpleNamespace(
        transactions=tx,
        prepare=AsyncMock(return_value=prepared),
        save_response=AsyncMock(),
        complete=AsyncMock(),
        routing=legacy_routes(),
    )
    gateway = SimpleNamespace(generate_messages=AsyncMock())
    with pytest.raises(CheckpointConflict):
        await graph.execute_step(
            tx, run_id, 3, 7, cp.owner, gateway, Mock(), Mock(), serial_runtime=runtime
        )
    gateway.generate_messages.assert_not_called()
    runtime.save_response.assert_not_called()
    runtime.complete.assert_not_called()
    available = pool.try_acquire("first", evaluation=True)
    assert available is not None
    available.release()
