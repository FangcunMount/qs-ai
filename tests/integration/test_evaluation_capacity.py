"""Real MySQL admission races; no provider or production data involved."""

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from qs_ai.application.evaluation.capacity import CapacityExceeded, EvaluationCapacityPolicy
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_admission_locks,
    evaluation_capacity_reservations,
    evaluation_checkpoints,
    evaluation_run_policies,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run

pytestmark = pytest.mark.integration
AT = datetime(2026, 9, 12, 23, 59, tzinfo=UTC)
policy = load_execution_policy()
CALLS = policy.generation_per_run + policy.semantic_per_run


@pytest.fixture
async def batch(setup_run):
    tx, _, release = setup_run
    org = uuid4().int % 1_000_000_000 + 100
    scopes = [ManagementScope(uuid4(), org, 42) for _ in range(3)]
    async with tx.open() as db:
        for scope in scopes:
            await create_run(db, scope.run_id, release, org, scope.actor, "预算验证", AT)
        await db.commit()
    try:
        yield tx, scopes
    finally:
        async with tx.open() as db:
            for table in (
                evaluation_capacity_reservations,
                evaluation_checkpoints,
                evaluation_run_policies,
                evaluation_runs,
            ):
                await db.execute(
                    delete(table).where(table.c.run_id.in_([str(s.run_id) for s in scopes]))
                )
            await db.execute(
                delete(evaluation_admission_locks).where(
                    evaluation_admission_locks.c.organization_id == org
                )
            )
            await db.commit()


async def reservations(tx, org):
    async with tx.open() as db:
        return (
            (
                await db.execute(
                    select(evaluation_capacity_reservations).where(
                        evaluation_capacity_reservations.c.organization_id == org
                    )
                )
            )
            .mappings()
            .all()
        )


async def start(store, scope, at=AT):
    return await store.start(scope, 1, "启动预算验证", at, confirm=True)


@pytest.mark.parametrize("kind", ["daily", "active"])
async def test_competing_starts_admit_only_one_without_partial_transition(batch, kind):
    tx, scopes = batch
    limits = EvaluationCapacityPolicy(
        CALLS if kind == "daily" else CALLS * 3, 3 if kind == "daily" else 1
    )
    store = MySQLEvaluationManagement(tx, limits)
    results = await asyncio.gather(*(start(store, s) for s in scopes), return_exceptions=True)
    assert sum(isinstance(r, CapacityExceeded) for r in results) == 2
    assert sum(not isinstance(r, Exception) for r in results) == 1
    ledger = await reservations(tx, scopes[0].organization_id)
    assert len(ledger) == 1
    assert ledger[0]["provider_calls"] == CALLS
    for scope, result in zip(scopes, results, strict=True):
        view = await store.get(scope)
        assert (view.status, view.version) == (
            ("requested", 1) if isinstance(result, CapacityExceeded) else ("collecting", 2)
        )


async def test_cancel_releases_active_capacity_but_does_not_refund_daily_budget(batch):
    tx, (one, two, _) = batch
    store = MySQLEvaluationManagement(tx, EvaluationCapacityPolicy(CALLS, 1))
    await start(store, one)
    await store.cancel(one, 2, "取消测试", AT, discard=False, confirm=True)
    with pytest.raises(CapacityExceeded):
        await start(store, two)
    assert (await store.get(two)).version == 1
    assert len(await reservations(tx, one.organization_id)) == 1
    # New UTC day releases daily allowance, retaining yesterday's receipt.
    await start(store, two, AT + timedelta(minutes=2))
    ledger = await reservations(tx, one.organization_id)
    assert len(ledger) == 2
    assert len({r["budget_day"] for r in ledger}) == 2


async def test_utc_day_not_server_timezone_and_repeated_start_never_charges_twice(batch):
    tx, (scope, _, _) = batch
    store = MySQLEvaluationManagement(tx, EvaluationCapacityPolicy(CALLS, 1))
    await start(store, scope, AT.astimezone(timezone(timedelta(hours=8))))
    ledger = await reservations(tx, scope.organization_id)
    assert ledger[0]["budget_day"] == AT.date()
    assert ledger[0]["reserved_at"] == AT.replace(tzinfo=None)
    before = await rows(tx, scope.run_id)
    from qs_ai.application.evaluation.checkpoints import CheckpointConflict

    with pytest.raises(CheckpointConflict):
        await start(store, scope)
    assert await reservations(tx, scope.organization_id) == ledger
    assert await rows(tx, scope.run_id) == before


async def test_invalid_confirmation_and_insufficient_budget_leave_no_reservation(batch):
    tx, (scope, _, _) = batch
    store = MySQLEvaluationManagement(tx, EvaluationCapacityPolicy(CALLS - 1, 1))
    before = await rows(tx, scope.run_id)
    with pytest.raises(ValueError):
        await store.start(scope, 1, "启动", AT, confirm=False)
    with pytest.raises(CapacityExceeded):
        await start(store, scope)
    assert await rows(tx, scope.run_id) == before
    assert not await reservations(tx, scope.organization_id)


async def test_capacity_read_is_scoped_read_only_and_reports_current_limits(batch):
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.evaluation_capacity import MySQLEvaluationCapacity

    tx, (scope, _, _) = batch
    store = MySQLEvaluationManagement(tx, EvaluationCapacityPolicy(CALLS * 2, 1))
    await start(store, scope)
    before = await reservations(tx, scope.organization_id)
    reader = MySQLEvaluationCapacity(tx, EvaluationCapacityPolicy(CALLS - 1, 1))
    current = await reader.get(DraftScope(scope.organization_id, 42), AT)
    assert (
        current.reserved_provider_calls,
        current.remaining_provider_calls,
        current.remaining_full_runs,
        current.active_runs,
    ) == (CALLS, 0, 0, 1)
    assert current.reservation_count == 1
    assert current.reservations[0].run_id == str(scope.run_id)
    assert not current.reservations_truncated
    foreign = await reader.get(DraftScope(scope.organization_id + 1, 42), AT)
    assert (foreign.reserved_provider_calls, foreign.active_runs) == (0, 0)
    assert not foreign.reservations
    assert await reservations(tx, scope.organization_id) == before


async def test_capacity_estimate_uses_registered_policy_without_file_reads(batch, monkeypatch):
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.evaluation_capacity import MySQLEvaluationCapacity

    tx, scopes = batch

    def forbidden(*args, **kwargs):
        raise AssertionError("Capacity lookup read a policy file")

    monkeypatch.setattr(
        "qs_ai.infrastructure.qs_server.evaluation_policies.load_execution_policy", forbidden
    )
    result = await MySQLEvaluationCapacity(tx, EvaluationCapacityPolicy()).get(
        DraftScope(scopes[0].organization_id, 42), AT
    )
    assert result.full_run_provider_calls == CALLS
    assert result.remaining_provider_calls == 1024
