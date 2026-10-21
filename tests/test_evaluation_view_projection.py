"""Original evaluation view formatting from a projected root snapshot."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql import evaluation_management as management


async def test_view_projection_preserves_original_receipts_and_never_mutates_source(monkeypatch):
    scope = ManagementScope(uuid4(), 7, 9)
    row = {
        "run_id": str(scope.run_id),
        "organization_id": 7,
        "version": 2,
        "checkpoint_json": None,
        "progress_json": {"status": "collecting", "transitions": []},
        "execution_mode": "serial_v1",
    }
    before = copy.deepcopy(row)
    cancel = AsyncMock(return_value=("", row))
    final = AsyncMock(return_value=("", False))
    receipt = Mock(return_value="first-creation-receipt")
    monkeypatch.setattr(management, "read_cancellation", cancel)
    monkeypatch.setattr(management, "read_finalization", final)
    monkeypatch.setattr(management, "creation_receipt", receipt)
    db = SimpleNamespace(execute=AsyncMock())
    view = await management.view_from_snapshot(db, scope, row)
    assert view.run_id == str(scope.run_id) and view.version == 2 and view.status == "collecting"
    assert view.creation_json == "first-creation-receipt"
    assert row == before
    cancel.assert_awaited_once()
    final.assert_awaited_once()
    db.execute.assert_not_called()


async def test_projected_view_cannot_disclose_another_run_or_organization():
    scope = ManagementScope(uuid4(), 7, 9)
    db = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(NotFound):
        await management.view_from_snapshot(
            db, scope, {"run_id": str(scope.run_id), "organization_id": 8}
        )
    db.execute.assert_not_called()
