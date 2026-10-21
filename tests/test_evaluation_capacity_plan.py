"""Original capacity math and reservation are shared with no-write planning."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.capacity import CapacityExceeded, EvaluationCapacityPolicy
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql import evaluation_capacity as capacity


def fixture(monkeypatch, *, active=0, used=0, prior=None):
    scope, at = ManagementScope(uuid4(), 7, 9), datetime(2026, 10, 21, tzinfo=UTC)
    policy = EvaluationCapacityPolicy()
    read_policy = AsyncMock(return_value=(policy, {"revision": 1}))
    monkeypatch.setattr(capacity, "evaluation_policy", read_policy)
    frozen = SimpleNamespace(
        definition_json="frozen",
        fingerprint="sha256:" + "a" * 64,
        generation_per_run=2,
        semantic_per_run=3,
    )
    monkeypatch.setattr(capacity, "frozen_policies", lambda _: (frozen, None))

    def scalar(value):
        result = Mock()
        result.scalar_one.return_value = value
        return result

    previous, stored = Mock(), Mock()
    previous.mappings.return_value.one_or_none.return_value = prior
    stored.mappings.return_value.one.return_value = {
        "definition_json": frozen.definition_json,
        "fingerprint": frozen.fingerprint,
    }
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[scalar(active), previous, scalar("{}"), stored, scalar(used), Mock()]
        ),
        commit=AsyncMock(),
    )
    return SimpleNamespace(**locals())


async def test_original_admission_retains_exact_reservation_and_caller_commit(monkeypatch):
    f = fixture(monkeypatch)
    await capacity.admit(f.db, f.scope, f.policy, f.at)
    assert f.db.execute.await_count == 6
    statement = f.db.execute.await_args.args[0]
    assert statement.is_insert
    params = statement.compile().params
    assert params["provider_calls"] == 5 and params["daily_limit"] == f.policy.daily_provider_calls
    assert params["requested_by"] == "user:9" and params["reserved_at"] == f.at.replace(tzinfo=None)
    f.db.commit.assert_not_called()


async def test_projection_uses_original_math_without_reservation_or_quota_row_locks(monkeypatch):
    f = fixture(monkeypatch)
    values = await capacity.plan_admission(f.db, f.scope, f.policy, f.at)
    assert values["provider_calls"] == 5 and values["quota_snapshot"] == {"revision": 1}
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in f.db.execute.await_args_list
    )
    assert f.read_policy.await_args.kwargs == {"lock": False}
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("limit", ["active", "daily"])
async def test_projection_retains_original_capacity_refusal(limit, monkeypatch):
    f = fixture(
        monkeypatch, active=100 if limit == "active" else 0, used=100000 if limit == "daily" else 0
    )
    with pytest.raises(CapacityExceeded):
        await capacity.plan_admission(f.db, f.scope, f.policy, f.at)
    assert all(c.args[0].is_select for c in f.db.execute.await_args_list)


async def test_projection_reuses_original_run_reservation_without_creating_another(monkeypatch):
    f = fixture(monkeypatch, prior={"organization_id": 7})
    assert await capacity.plan_admission(f.db, f.scope, f.policy, f.at) is None
    assert f.db.execute.await_count == 2
