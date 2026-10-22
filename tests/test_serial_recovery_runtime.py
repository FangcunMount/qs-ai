"""Recovery routes existing evidence after closing observation; no provider capability exists."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.workflow_transport import serial_recovery_runtime as module
from qs_ai.infrastructure.workflow_transport.serial_generation_sender import SerialGenerationPending
from tests.test_evaluation_serial_response_receipts import original as original


def fixture(original, monkeypatch, *, kind="generation", phase="dispatching", response=True):
    p = original
    cp = replace(p.cp, kind=kind, candidate_id="candidate:first" if kind == "semantic" else "")
    if phase == "prepared":
        cp = replace(cp, phase="prepared", dispatch_started_at=None)
    reference = replace(p.ref, checkpoint=cp) if phase == "dispatching" else None
    db = SimpleNamespace(connection=AsyncMock(), commit=AsyncMock(), execute=AsyncMock())
    active = False
    opens = []

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        opens.append(True)
        try:
            yield db
        finally:
            active = False

    def is_open():
        return active

    async def source(*args, **kwargs):
        assert active and db.connection.await_count == 1 and kwargs["lock"] is False
        return SimpleNamespace(checkpoint=cp, run={"execution_mode": "serial_v1"})

    reader = AsyncMock(side_effect=source)
    references = AsyncMock(return_value=reference if response else None)
    monkeypatch.setattr(module, "read_expired_recovery_source", reader)
    monkeypatch.setattr(module, "read_serial_response_reference", references)
    tx = SimpleNamespace(open=opened)
    runtime = module.SerialRecoveryRuntime(
        tx,
        Mock(),
        Mock(),
        Mock(),
        signing_key=None,
        recipient_key=None,
    )
    assert not opens and db.connection.await_count == 0

    async def after_read(*args):
        assert not active
        return True

    expired, generation, semantic = [AsyncMock(side_effect=after_read) for _ in range(3)]
    runtime._expired, runtime._generation, runtime._semantic = expired, generation, semantic

    async def call():
        return await runtime(
            p.ref.run_id,
            p.ref.version,
            p.ref.organization_id,
            cp.invocation_id,
            cp.lease_expires_at,
            cp.lease_expires_at,
            "worker:recovery",
        )

    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_first_durable_response_uses_original_terminal_sender_after_snapshot_closed(
    original, monkeypatch, kind
):
    f = fixture(original, monkeypatch, kind=kind)
    assert await f.call()
    target = f.generation if kind == "generation" else f.semantic
    target.assert_awaited_once_with(f.reference)
    (f.semantic if kind == "generation" else f.generation).assert_not_called()
    f.expired.assert_not_called()
    assert len(f.opens) == 1
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("phase", ["prepared", "dispatching"])
async def test_without_response_only_original_expiry_sender_runs(original, monkeypatch, phase):
    f = fixture(original, monkeypatch, phase=phase, response=False)
    assert await f.call()
    request, actor, at = f.expired.await_args.args
    assert request.invocation_id == f.cp.invocation_id and request.version == original.ref.version
    assert actor == "worker:recovery" and at == f.cp.lease_expires_at
    f.generation.assert_not_called()
    f.semantic.assert_not_called()
    if phase == "prepared":
        f.references.assert_not_called()
    f.db.commit.assert_not_called()


async def test_unresolved_original_response_does_not_fall_back_to_unknown_or_business_retry(
    original, monkeypatch
):
    f = fixture(original, monkeypatch)
    f.generation.side_effect = SerialGenerationPending("Original finalization unresolved")
    with pytest.raises(SerialGenerationPending):
        await f.call()
    f.expired.assert_not_called()
    f.semantic.assert_not_called()
    assert f.generation.await_count == 1
    f.db.commit.assert_not_called()


async def test_changed_original_ownership_cannot_reach_either_business_sender(
    original, monkeypatch
):
    f = fixture(original, monkeypatch)
    f.reader.side_effect = CheckpointConflict("Original expiry changed")
    with pytest.raises(CheckpointConflict):
        await f.call()
    f.expired.assert_not_called()
    f.generation.assert_not_called()
    f.semantic.assert_not_called()
    f.references.assert_not_called()


@pytest.mark.parametrize("budget", [0, -1, float("nan"), float("inf")])
def test_runtime_rejects_unbounded_database_budget_before_resource_creation(budget):
    with pytest.raises(ValueError):
        module.SerialRecoveryRuntime(
            Mock(),
            Mock(),
            Mock(),
            Mock(),
            signing_key=None,
            recipient_key=None,
            database_seconds=budget,
        )
