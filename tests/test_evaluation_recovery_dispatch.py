"""Protect bounded original scan and ownership before adding transaction dispatch."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_scan as scan
from tests.test_evaluation_expired_recovery_source import source as source


def fixture(source, monkeypatch):
    p = source
    row = dict(
        run_id=str(p.run_id),
        organization_id=1,
        version=7,
        checkpoint_json=p.checkpoint["checkpoint_json"],
    )
    selected = Mock()
    selected.mappings.return_value.all.return_value = [row]
    db = SimpleNamespace(execute=AsyncMock(return_value=selected), commit=AsyncMock())
    active = False
    opened = []

    @asynccontextmanager
    async def open_session():
        nonlocal active
        active = True
        opened.append(True)
        try:
            yield db
        finally:
            active = False

    async def original(*args, **kwargs):
        assert active

    def is_open():
        return active

    recover = AsyncMock(side_effect=original)
    monkeypatch.setattr(scan, "recover_expired", recover)
    diagnostics = Mock()
    monkeypatch.setattr(scan, "emit", diagnostics)
    tx = SimpleNamespace(open=open_session)
    cursor = scan.RecoveryCursor()
    routes, schemas = object(), object()

    async def call(**kwargs):
        return await scan.recover_next(
            tx, routes, schemas, "worker:first", p.cp.lease_expires_at, cursor, **kwargs
        )

    return SimpleNamespace(**locals())


async def test_original_scan_closes_selection_then_commits_one_exact_recovery(source, monkeypatch):
    f = fixture(source, monkeypatch)
    assert await f.call()
    assert len(f.opened) == 2
    f.recover.assert_awaited_once_with(
        f.db,
        source.run_id,
        7,
        1,
        source.cp.invocation_id,
        source.cp.lease_expires_at,
        source.cp.lease_expires_at,
        "worker:first",
        f.routes,
        f.schemas,
    )
    f.db.commit.assert_awaited_once()
    assert f.cursor.after_run_id == str(source.run_id)
    statement = f.db.execute.await_args.args[0]
    assert statement._limit_clause.value == 64


async def test_original_scan_conflict_does_not_commit_and_retains_bounded_cursor(
    source, monkeypatch
):
    f = fixture(source, monkeypatch)
    f.recover.side_effect = CheckpointConflict("another owner won")
    assert not await f.call()
    f.db.commit.assert_not_called()
    f.diagnostics.assert_not_called()
    assert f.cursor.after_run_id == ""


@pytest.mark.parametrize("limit", [0, 257, True])
async def test_scan_rejects_invalid_bound_before_reading(source, monkeypatch, limit):
    f = fixture(source, monkeypatch)
    with pytest.raises(ValueError):
        await f.call(batch_size=limit)
    f.db.execute.assert_not_called()


@pytest.mark.parametrize("outcome", ["committed", "unchanged", "conflict", "pending"])
async def test_transaction_scan_does_not_open_legacy_root_or_grant_business_retry(
    source, monkeypatch, outcome
):
    f = fixture(source, monkeypatch)

    async def dispatched(*args):
        assert not f.is_open()
        assert args == (
            source.run_id,
            7,
            1,
            source.cp.invocation_id,
            source.cp.lease_expires_at,
            source.cp.lease_expires_at,
            "worker:first",
        )
        if outcome == "conflict":
            raise CheckpointConflict("original fence changed")
        if outcome == "pending":
            raise RuntimeError("original responsibility unresolved")
        return outcome == "committed"

    dispatch = AsyncMock(side_effect=dispatched)
    if outcome == "pending":
        with pytest.raises(RuntimeError):
            await f.call(dispatch=dispatch)
    else:
        assert await f.call(dispatch=dispatch) is (outcome == "committed")
    assert len(f.opened) == 1
    f.recover.assert_not_called()
    f.db.commit.assert_not_called()
    assert f.diagnostics.call_count == (outcome == "committed")
