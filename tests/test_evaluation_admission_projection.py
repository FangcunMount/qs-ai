"""Actual shared evaluation rules compose a read-only view before half messages."""

import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.capacity import CapacityExceeded
from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.infrastructure.persistence.mysql import evaluation_admission as projection
from tests.test_evaluation_start_plan import fixture as original


def fixture(monkeypatch):
    f = original()
    source = {**f.run, "version": 1, "checkpoint_json": None, "execution_mode": "serial_v1"}
    creation = json.loads(source["definition_json"])
    creation["release_fingerprint"] = "sha256:" + "a" * 64
    source["definition_json"] = json.dumps(creation)
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = source
    db = SimpleNamespace(
        execute=AsyncMock(return_value=result),
        in_transaction=Mock(return_value=True),
        commit=AsyncMock(),
    )

    @asynccontextmanager
    async def opened():
        yield db

    tx = SimpleNamespace(open=opened)
    scope = ManagementScope(f.run_id, 7, 9)

    async def view(db, scope, row, limits):
        return EvaluationView(
            run_id=str(scope.run_id),
            version=row["version"],
            status=row["progress_json"]["status"],
            unresolved_result_unknown_count=0,
            resolutions_json="[]",
        )

    monkeypatch.setattr(projection, "view_from_snapshot", AsyncMock(side_effect=view))
    models = AsyncMock()
    budget = AsyncMock(return_value={"provider_calls": 5})
    monkeypatch.setattr(projection, "check_editable_models", models)
    monkeypatch.setattr(projection, "run_release", lambda value: value)
    monkeypatch.setattr(projection, "plan_admission", budget)
    store = projection.EvaluationAdmissionProjection(tx)
    return SimpleNamespace(**locals())


async def test_start_uses_original_transition_and_capacity_without_locks_or_business_writes(
    monkeypatch,
):
    f = fixture(monkeypatch)
    before = copy.deepcopy(f.source)
    view = await f.store.start(f.scope, 1, "explicit", f.f.at, confirm=True)
    assert view.version == 2 and view.status == "collecting"
    assert f.store.state.version == 2 and f.store.state.status == "collecting"
    assert f.store.state.release_fingerprint == "sha256:" + "a" * 64
    assert f.store.source_digest == projection.evaluation_source_digest(f.source)
    assert f.source == before
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in f.db.execute.await_args_list
    )
    f.budget.assert_awaited_once()
    f.models.assert_awaited_once()
    f.db.commit.assert_not_called()


async def test_capacity_refusal_cannot_expose_planned_state_as_committed_business(monkeypatch):
    f = fixture(monkeypatch)
    f.budget.side_effect = CapacityExceeded("daily limit")
    with pytest.raises(CapacityExceeded):
        await f.store.start(f.scope, 1, "explicit", f.f.at, confirm=True)
    assert f.store.state is None and f.store.source_digest is not None
    f.db.commit.assert_not_called()


async def test_cancel_composes_original_plan_and_view_without_staging_checkpoint(monkeypatch):
    from qs_ai.application.evaluation.checkpoints import CheckpointState

    f = fixture(monkeypatch)
    planned = AsyncMock(
        return_value=(
            CheckpointState(f.scope.run_id, 2, None),
            {"status": "canceled", "transitions": []},
        )
    )
    monkeypatch.setattr(projection, "plan_cancellation", planned)
    view = await f.store.cancel(f.scope, 1, "explicit", f.f.at, discard=False, confirm=True)
    assert view.status == "canceled" and f.store.state.status == "canceled"
    planned.assert_awaited_once_with(
        f.db, f.scope, 1, "explicit", f.f.at, discard=False, confirm=True
    )
    f.budget.assert_not_called()
    f.db.commit.assert_not_called()


async def test_projection_instance_cannot_be_reused_as_another_mutation(monkeypatch):
    f = fixture(monkeypatch)
    await f.store.start(f.scope, 1, "explicit", f.f.at, confirm=True)
    with pytest.raises(RuntimeError):
        await f.store.start(f.scope, 1, "explicit", f.f.at, confirm=True)
