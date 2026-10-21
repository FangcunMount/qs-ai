"""Preserve worker selection/budgets and explicit root ownership at the new seam."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_worker as worker
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_dispatch import (
    PreflightTransactionPending,
)


def fixture(monkeypatch):
    run_id, at = uuid4(), datetime(2026, 10, 21, tzinfo=UTC)
    row = {
        "run_id": str(run_id),
        "organization_id": 7,
        "version": 2,
        "progress_json": {"status": "collecting"},
        "execution_mode": "serial_v1",
    }
    selected = Mock()
    selected.mappings.return_value.one_or_none.return_value = row
    db = SimpleNamespace(execute=AsyncMock(return_value=selected), commit=AsyncMock())
    opened = []

    @asynccontextmanager
    async def transactions():
        opened.append(True)
        yield db

    legacy = AsyncMock()
    step = AsyncMock()
    monkeypatch.setattr(worker, "recover_next", AsyncMock(return_value=False))
    monkeypatch.setattr(worker, "recover_next_candidate", AsyncMock(return_value=False))
    monkeypatch.setattr(worker, "execute_preflight", legacy)
    monkeypatch.setattr(worker, "execute_step", step)
    gateway = SimpleNamespace(generate_messages=AsyncMock())

    def create(**kwargs):
        value = worker.EvaluationWorker(
            SimpleNamespace(open=transactions),
            gateway,
            Mock(),
            Mock(),
            "worker:original",
            enabled=True,
            clock=lambda: at,
            **kwargs,
        )
        value._drain_cancellations = AsyncMock(return_value=False)
        return value

    return SimpleNamespace(**locals())


async def test_explicit_transaction_preflight_owns_its_root_and_never_calls_legacy_commit_hook(
    monkeypatch,
):
    f = fixture(monkeypatch)
    dispatch = AsyncMock()
    consumer = f.create(preflight_dispatch=dispatch)
    assert await consumer.once()
    dispatch.assert_awaited_once_with(f.run_id, 2, 7)
    assert len(f.opened) == 1
    f.db.commit.assert_not_called()
    f.legacy.assert_not_called()
    f.step.assert_not_called()
    f.gateway.generate_messages.assert_not_called()


async def test_default_worker_retains_original_preflight_and_host_root_commit(monkeypatch):
    f = fixture(monkeypatch)
    assert await f.create().once()
    f.legacy.assert_awaited_once_with(f.db, f.run_id, 2, 7, f.at)
    f.db.commit.assert_awaited_once()
    assert len(f.opened) == 2
    f.step.assert_not_called()


@pytest.mark.parametrize("failure", [PreflightTransactionPending, CheckpointConflict])
async def test_unresolved_transaction_never_authorizes_generation_or_an_extra_commit(
    monkeypatch, failure
):
    f = fixture(monkeypatch)
    dispatch = AsyncMock(side_effect=failure("original state remains unresolved"))
    consumer = f.create(preflight_dispatch=dispatch)
    if failure is CheckpointConflict:
        assert not await consumer.once()
    else:
        with pytest.raises(failure):
            await consumer.once()
    f.legacy.assert_not_called()
    f.step.assert_not_called()
    f.db.commit.assert_not_called()
