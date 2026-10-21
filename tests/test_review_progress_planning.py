"""The original review writer keeps locked planning, CAS and caller-owned commit."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql import evaluation_reviews as reviews
from tests.test_evaluation_review import review


@pytest.mark.parametrize("conflict", [False, True])
async def test_original_review_adapter_uses_locked_plan_and_never_owns_root(monkeypatch, conflict):
    scope = ManagementScope(uuid4(), 7, 42)
    values = (review(),)
    state = CheckpointState(scope.run_id, 10, None)
    progress = {"status": "awaiting_review", "human_reviews": [{"original": "frozen"}]}
    plan = AsyncMock(return_value=(state, progress))
    save = AsyncMock()
    db = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(reviews, "plan_reviews", plan)
    monkeypatch.setattr(reviews, "save_checkpoint", save)
    if conflict:
        original = CheckpointConflict("Original version changed")
        plan.side_effect = original
        with pytest.raises(CheckpointConflict) as caught:
            await reviews.accept_reviews(db, scope, 9, values)
        assert caught.value is original
        save.assert_not_called()
        db.execute.assert_not_called()
    else:
        assert await reviews.accept_reviews(db, scope, 9, values) is state
        save.assert_awaited_once_with(db, state, 9)
        statement = db.execute.await_args.args[0]
        assert statement.is_update and statement.table.name == "evaluation_runs"
        assert statement.compile().params["progress_json"] is progress
    plan.assert_awaited_once_with(db, scope, 9, values, lock=True)
    db.commit.assert_not_called()
