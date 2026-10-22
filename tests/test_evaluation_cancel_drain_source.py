"""Characterize original drain admission and root ownership before transaction adaptation."""

import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql import evaluation_cancellation as original


@pytest.fixture(params=["root", "readonly"])
def source(monkeypatch, request):
    boundary = request.param
    scope = ManagementScope(uuid4(), 7, 42)
    at = datetime(2026, 10, 22, tzinfo=UTC)
    checkpoint = {"version": 8, "checkpoint_json": None}
    intent = {
        "actor": "user:9",
        "reason": "original requested cancellation",
        "requested_at": at.isoformat(),
    }
    progress = {"status": "collecting", "cancel_requested": intent}
    run = {"progress_json": progress}
    results = []
    for row in (checkpoint, run):
        result = Mock()
        result.mappings.return_value.one.return_value = row
        results.append(result)
    db = SimpleNamespace(execute=AsyncMock(side_effect=results), commit=AsyncMock())
    active, cancel = AsyncMock(return_value=()), AsyncMock()
    monkeypatch.setattr(original, "active_claims", active)
    monkeypatch.setattr(original, "cancel", cancel)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("before_request", [False, True])
async def test_original_drain_retains_request_actor_reason_time_floor_and_caller_root(
    source, before_request
):
    p = source
    at = p.at + timedelta(seconds=-1 if before_request else 1)
    before = copy.deepcopy((p.checkpoint, p.run))
    if p.boundary == "root":
        assert await original.finish_cancellation(p.db, p.scope, at)
        p.cancel.assert_awaited_once_with(
            p.db,
            ManagementScope(p.scope.run_id, 7, 9),
            8,
            p.intent["reason"],
            max(at, p.at),
            discard=False,
            confirm=True,
        )
    else:
        found = await original.read_cancel_drain_source(p.db, p.scope, at, lock=False)
        assert found == original.CancellationDrainSource(
            ManagementScope(p.scope.run_id, 7, 9), 8, p.intent["reason"], max(at, p.at)
        )
        assert p.intent["reason"] not in repr(found)
        p.cancel.assert_not_called()
    assert (p.checkpoint, p.run) == before
    assert len(p.db.execute.await_args_list) == 2
    assert all(
        c.args[0].is_select and (c.args[0]._for_update_arg is not None) == (p.boundary == "root")
        for c in p.db.execute.await_args_list
    )
    assert p.db.execute.await_args_list[1].args[0].compile().params["organization_id_1"] == 7
    p.db.commit.assert_not_called()


@pytest.mark.parametrize(
    "pending",
    ["missing_progress", "missing_intent", "already_canceled", "checkpoint", "claim", "unknown"],
)
async def test_original_drain_does_not_erase_active_or_unknown_responsibility(source, pending):
    p = source
    if pending == "missing_progress":
        p.run["progress_json"] = None
    elif pending == "missing_intent":
        p.progress.pop("cancel_requested")
    elif pending == "already_canceled":
        p.progress["status"] = "canceled"
    elif pending == "checkpoint":
        p.checkpoint["checkpoint_json"] = {"phase": "dispatching"}
    elif pending == "claim":
        p.active.return_value = (object(),)
    else:
        p.progress["unresolved_result_unknown_count"] = 1
    result = (
        await original.finish_cancellation(p.db, p.scope, p.at)
        if p.boundary == "root"
        else await original.read_cancel_drain_source(p.db, p.scope, p.at, lock=False)
    )
    assert not result
    p.cancel.assert_not_called()
    p.db.commit.assert_not_called()


async def test_original_drain_does_not_reassign_a_malformed_request_actor(source):
    p = source
    p.intent["actor"] = "system:unrelated"
    with pytest.raises(ValueError):
        if p.boundary == "root":
            await original.finish_cancellation(p.db, p.scope, p.at)
        else:
            await original.read_cancel_drain_source(p.db, p.scope, p.at, lock=False)
    p.cancel.assert_not_called()
    p.db.commit.assert_not_called()
