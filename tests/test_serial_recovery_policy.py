"""Explicit serial recovery policy never promotes Prepared/Committed to another business root."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import Execution, Reservation, State

from qs_ai.infrastructure.workflow_transport.serial_recovery_runtime import SerialRecoveryRuntime
from tests.test_evaluation_expired_recovery_source import source as source
from tests.test_evaluation_serial_response_receipts import original as original
from tests.test_expired_recovery_sender import fixture as expiry_fixture
from tests.test_serial_generation_state import frozen as generation_frozen
from tests.test_serial_semantic_state import frozen as semantic_frozen


@pytest.fixture(params=[generation_frozen, semantic_frozen], ids=["generation", "semantic"])
def frozen(request, original):
    return request.param.__wrapped__(original)


@pytest.mark.parametrize("status", [State.PREPARED, State.COMMITTED, State.ABORTED])
@pytest.mark.parametrize("advanced", [False, True])
async def test_serial_response_policy_requires_exact_abort_and_original_reference(
    frozen, status, advanced
):
    f = frozen
    execution = Execution(f.winner, 1, "m3-1", b"x" * 32, f.facts.at + timedelta(seconds=60))
    lookup = AsyncMock(return_value=Reservation(execution, False, status))
    owner = SimpleNamespace(store=SimpleNamespace(lookup=lookup))
    runtime = SerialRecoveryRuntime(
        Mock(), Mock(), owner, Mock(), signing_key=None, recipient_key=None, recover_aborted=True
    )
    runtime._generation_planner.intent = AsyncMock(return_value=f.intent)
    runtime._semantic_planner.intent = AsyncMock(return_value=f.intent)
    runtime._generation, runtime._semantic = AsyncMock(), AsyncMock()
    reference = f.operation.reference
    selected = (
        runtime._generation if reference.checkpoint.kind == "generation" else runtime._semantic
    )
    await runtime._finish_response(reference, f.facts.source_version + int(advanced))
    if status != State.ABORTED:
        selected.assert_awaited_once_with(reference)
        selected.retry.assert_not_called()
        selected.replan.assert_not_called()
    else:
        selected.assert_not_called()
        (selected.replan if advanced else selected.retry).assert_awaited_once_with(
            reference, execution
        )
    (
        runtime._semantic if reference.checkpoint.kind == "generation" else runtime._generation
    ).assert_not_called()


async def test_regressed_serial_source_never_authorizes_new_plan(frozen):
    f = frozen
    execution = Execution(f.winner, 1, "m3-1", b"x" * 32, f.facts.at + timedelta(seconds=60))
    owner = SimpleNamespace(
        store=SimpleNamespace(
            lookup=AsyncMock(return_value=Reservation(execution, False, State.ABORTED))
        )
    )
    runtime = SerialRecoveryRuntime(
        Mock(), Mock(), owner, Mock(), signing_key=None, recipient_key=None, recover_aborted=True
    )
    runtime._generation_planner.intent = AsyncMock(return_value=f.intent)
    runtime._semantic_planner.intent = AsyncMock(return_value=f.intent)
    runtime._generation, runtime._semantic = AsyncMock(), AsyncMock()
    with pytest.raises(MessageConflict):
        await runtime._finish_response(f.operation.reference, f.facts.source_version - 1)
    for value in (runtime._generation, runtime._semantic):
        value.assert_not_called()
        value.retry.assert_not_called()
        value.replan.assert_not_called()


@pytest.mark.parametrize("status", [State.PREPARED, State.COMMITTED, State.ABORTED])
async def test_interrupted_expiry_retry_keeps_first_actor_clock_and_frozen_wire(
    source, monkeypatch, status
):
    f = await expiry_fixture(source, monkeypatch)
    f.store.lookup.return_value = Reservation(f.execution, False, status)
    runtime = SerialRecoveryRuntime(
        f.tx,
        f.archives,
        f.runtime,
        Mock(),
        signing_key=None,
        recipient_key=None,
        recover_aborted=True,
    )
    runtime._expired = AsyncMock(return_value=True)
    runtime._expired.retry = AsyncMock(return_value=True)
    assert await runtime._finish_expiry(f.p.request, "worker:replacement", f.facts.at)
    if status == State.ABORTED:
        runtime._expired.retry.assert_awaited_once_with(f.p.request, f.execution)
        runtime._expired.assert_not_called()
    else:
        runtime._expired.assert_awaited_once_with(f.p.request, "worker:replacement", f.facts.at)
        runtime._expired.retry.assert_not_called()
