"""Actual candidate Run readonly projection preserves the original active-slot budget."""

from datetime import timedelta

import pytest

from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import (
    candidate_plan,
    prepare_candidate,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import active_claims, lock_run
from tests.integration.test_evaluation_candidate_execution import parallel_run as parallel_run
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT
from tests.integration.test_evaluation_step import persisted_assets as persisted_assets
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("claimed", [False, True])
@pytest.mark.parametrize("limit", [1, 3])
async def test_readonly_candidate_plan_matches_locked_root_without_changing_budget_or_facts(
    parallel_run, claimed, limit
):
    tx, run_id, *_ = parallel_run
    if claimed:
        async with tx.open() as db:
            await prepare_candidate(
                db,
                run_id,
                1,
                "worker:first",
                "execution:readonly",
                "invocation:readonly",
                AT,
                AT + timedelta(seconds=30),
                limit=limit,
            )
            await db.commit()
    before = await rows(tx, run_id)
    async with tx.open() as db:
        run, version = await lock_run(db, run_id, 1, lock=False)
        active = await active_claims(db, run_id, lock=False)
        observed = await candidate_plan(db, run_id, run, active, limit, lock=False)
    assert await rows(tx, run_id) == before
    assert version == (4 if claimed else 3)
    assert len(active) == claimed
    assert len(observed.ready) == max(0, limit - len(active))
    async with tx.open() as db:
        locked_run, locked_version = await lock_run(db, run_id, 1)
        locked_active = await active_claims(db, run_id)
        actual = await candidate_plan(db, run_id, locked_run, locked_active, limit)
    assert (locked_version, locked_active, actual) == (version, active, observed)
    assert await rows(tx, run_id) == before
    assert not (
        {(a.case_id, a.slot_ordinal) for a in actual.ready}
        & {(c.checkpoint.case_id, c.checkpoint.slot_ordinal) for c in active}
    )
