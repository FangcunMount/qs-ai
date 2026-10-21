"""Actual MySQL review planning is read-only and preserves original 35-item admission."""

from dataclasses import replace

import pytest
from sqlalchemy import event

from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews, plan_reviews
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count", [1, 35])
async def test_original_review_plan_only_reads_and_matches_later_authorized_commit(
    reviewable, count
):
    tx, scope, version, value = reviewable
    # Preserve the public per-item bound and the full original batch size; a
    # transaction metadata limit must not silently narrow valid review input.
    values = tuple(
        replace(value, candidate_id=f"candidate:{i}", reason="核" * 333)
        for i in range(1, count + 1)
    )
    before = await rows(tx, scope.run_id)
    evidence = await outputs(tx, scope.run_id)
    statements = []
    planning = False
    engine = tx.database.engine
    assert engine is not None

    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if planning:
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with tx.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            original = db.get_transaction()
            planning = True
            try:
                state, progress = await plan_reviews(db, scope, version, values, lock=False)
            finally:
                planning = False
            assert db.get_transaction() is original and original.is_active
            assert state.version == version + 1
            assert len(progress["human_reviews"]) == count
            assert all(
                v["reviewed_at"] == value.reviewed_at.isoformat() for v in progress["human_reviews"]
            )
        assert statements and all(s.lstrip().upper().startswith("SELECT") for s in statements)
        assert not any("FOR UPDATE" in s.upper() for s in statements)
        assert await rows(tx, scope.run_id) == before
        assert await outputs(tx, scope.run_id) == evidence
        async with tx.open() as db:
            committed = await accept_reviews(db, scope, version, values)
            await db.commit()
        actual = await rows(tx, scope.run_id)
        assert committed == state and actual[2]["version"] == state.version
        assert actual[0]["progress_json"] == progress
        assert await outputs(tx, scope.run_id) == evidence
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
