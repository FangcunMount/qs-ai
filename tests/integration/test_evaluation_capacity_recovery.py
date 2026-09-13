"""Manual recovery reuses the original reservation and cannot bypass admission."""

from dataclasses import replace
from datetime import UTC
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select

from qs_ai.application.evaluation.capacity import CapacityExceeded, EvaluationCapacityPolicy
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_capacity_reservations as ledger
from tests.integration.test_evaluation_capacity import CALLS
from tests.integration.test_evaluation_recovery import EXPIRY, pending, recover
from tests.integration.test_evaluation_resolution import decision
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("own_reservation", [False, True])
async def test_recovery_preserves_original_daily_reservation_and_refuses_unbudgeted_resume(
    ready, own_reservation
):
    tx, run_id, *_ = ready
    state = await pending(ready, dispatched=True, version=3)
    state = await recover(ready, state)
    scope = ManagementScope(run_id, 1, 42)
    reserved_run = str(run_id) if own_reservation else str(uuid4())
    async with tx.open() as db:
        await db.execute(
            insert(ledger).values(
                run_id=reserved_run,
                organization_id=1,
                budget_day=EXPIRY.astimezone(UTC).date(),
                provider_calls=CALLS,
                daily_limit=CALLS,
                requested_by=scope.actor,
                reserved_at=EXPIRY.astimezone(UTC).replace(tzinfo=None),
            )
        )
        await db.commit()
    try:
        before = await rows(tx, run_id)
        store = MySQLEvaluationManagement(tx, EvaluationCapacityPolicy(CALLS, 1))
        value = replace(decision(), actor=scope.actor)
        if own_reservation:
            assert (
                await store.resolve(scope, state.version, value, confirm=True)
            ).status == "collecting"
        else:
            with pytest.raises(CapacityExceeded):
                await store.resolve(scope, state.version, value, confirm=True)
            assert await rows(tx, run_id) == before
        async with tx.open() as db:
            saved = (
                (await db.execute(select(ledger).where(ledger.c.organization_id == 1)))
                .mappings()
                .all()
            )
        assert len(saved) == 1 and saved[0]["run_id"] == reserved_run
    finally:
        async with tx.open() as db:
            await db.execute(delete(ledger).where(ledger.c.run_id == reserved_run))
            await db.commit()
