"""Drain maintenance permission is explicit and cannot replace the original cancel request."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import Reservation, State

from qs_ai.infrastructure.workflow_transport import cancellation_drain_runtime as module
from qs_ai.infrastructure.workflow_transport.cancellation_drain_state import (
    read_cancellation_drain_plan,
)
from tests.test_cancellation_drain_sender import fixture as planning


@pytest.mark.parametrize("status", [State.PREPARED, State.COMMITTED, State.ABORTED])
@pytest.mark.parametrize("advanced", [False, True])
async def test_host_drain_policy_only_recovers_exact_abort_after_fresh_scoped_source(
    monkeypatch, status, advanced
):
    f = await planning(monkeypatch)
    facts, _, _ = read_cancellation_drain_plan(f.operation, f.winner)
    source = SimpleNamespace(version=facts.source_version + int(advanced), at=facts.at)
    header = AsyncMock(return_value={**f.p.run, **f.p.checkpoint})
    monkeypatch.setattr(module, "header", header)
    monkeypatch.setattr(module, "read_cancel_drain_source", AsyncMock(return_value=source))
    f.store.lookup.return_value = Reservation(f.execution, False, status)
    runtime = module.CancellationDrainRuntime(
        f.p.tx, f.archives, f.runtime, signing_key=None, recipient_key=None, recover_aborted=True
    )
    runtime._sender = AsyncMock(return_value=True)
    runtime._sender.retry, runtime._sender.replan = (
        AsyncMock(return_value=True),
        AsyncMock(return_value=True),
    )
    assert await runtime(f.p.scope, facts.at)
    if status != State.ABORTED:
        runtime._sender.assert_awaited_once_with(f.p.scope, facts.at)
        runtime._sender.retry.assert_not_called()
        runtime._sender.replan.assert_not_called()
    elif advanced:
        runtime._sender.replan.assert_awaited_once_with(f.operation, f.execution, facts.at)
        runtime._sender.retry.assert_not_called()
    else:
        runtime._sender.retry.assert_awaited_once_with(f.operation, f.execution)
        runtime._sender.replan.assert_not_called()


async def test_drain_policy_off_keeps_sender_contract_and_no_extra_observation(monkeypatch):
    f = await planning(monkeypatch)
    header = AsyncMock()
    monkeypatch.setattr(module, "header", header)
    runtime = module.CancellationDrainRuntime(
        f.p.tx, f.archives, f.runtime, signing_key=None, recipient_key=None
    )
    runtime._sender = AsyncMock(return_value=True)
    assert await runtime(f.p.scope, f.p.at)
    header.assert_not_called()
    f.store.lookup.assert_not_called()
    runtime._sender.retry.assert_not_called()
    runtime._sender.replan.assert_not_called()


async def test_regressed_drain_source_is_not_new_recovery_authority(monkeypatch):
    f = await planning(monkeypatch)
    facts, _, _ = read_cancellation_drain_plan(f.operation, f.winner)
    monkeypatch.setattr(module, "header", AsyncMock(return_value={**f.p.run, **f.p.checkpoint}))
    monkeypatch.setattr(
        module,
        "read_cancel_drain_source",
        AsyncMock(
            return_value=SimpleNamespace(
                version=facts.source_version - 1,
                at=facts.at,
            )
        ),
    )
    f.store.lookup.return_value = Reservation(f.execution, False, State.ABORTED)
    runtime = module.CancellationDrainRuntime(
        f.p.tx, f.archives, f.runtime, signing_key=None, recipient_key=None, recover_aborted=True
    )
    runtime._sender = AsyncMock()
    with pytest.raises(MessageConflict):
        await runtime(f.p.scope, facts.at)
    runtime._sender.retry.assert_not_called()
    runtime._sender.replan.assert_not_called()
