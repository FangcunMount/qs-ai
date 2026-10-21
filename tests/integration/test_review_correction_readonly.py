"""Original correction planning reads only, including replay after finalization."""

from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import event

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_review_corrections import (
    plan_review_correction,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_finalization import passing_semantics as passing_semantics
from tests.integration.test_evaluation_review_corrections import intent
from tests.integration.test_evaluation_review_corrections import signatures as signatures
from tests.integration.test_evaluation_reviews import outputs
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("case", ["new", "replay", "different_intent", "finalized_replay"])
async def test_correction_readonly_plan_matches_original_new_write_or_exact_historical_replay(
    signatures, case
):
    tx, scope, _, _, _, store = signatures
    args = intent(signatures)
    if case != "new":
        corrected = await store.correct_review(**args)
        if case == "finalized_replay":
            finished = await store.finalize(
                scope,
                corrected.version,
                True,
                "原完整审核验收",
                args["value"].reviewed_at + timedelta(seconds=1),
                confirm=True,
            )
            assert finished.status == "approved"
    before, evidence = await rows(tx, scope.run_id), await outputs(tx, scope.run_id)
    engine = tx.database.engine
    assert engine is not None
    statements = []
    planning = False

    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if planning:
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with tx.open() as db:
            run = await header(db, scope)
            original = db.get_transaction()
            planning = True
            try:
                if case == "different_intent":
                    with pytest.raises(CheckpointConflict):
                        await plan_review_correction(
                            db,
                            **{**args, "value": replace(args["value"], reason="不同授权输入")},
                            run=run,
                        )
                    projected = None
                else:
                    projected = await plan_review_correction(db, **args, run=run)
            finally:
                planning = False
            assert db.get_transaction() is original and original.is_active
        assert all(s.lstrip().upper().startswith("SELECT") for s in statements)
        assert not any("FOR UPDATE" in s.upper() for s in statements)
        assert await rows(tx, scope.run_id) == before
        assert await outputs(tx, scope.run_id) == evidence
        if case == "new":
            assert projected is not None and len(projected["review_corrections"]) == 1
            response = await store.correct_review(**args)
            assert response.version == args["expected_version"] + 1
            assert (await rows(tx, scope.run_id))[0]["progress_json"] == projected
        elif case in ("replay", "finalized_replay"):
            assert projected is None
            assert await store.correct_review(**args) == await store.get(scope)
            assert await rows(tx, scope.run_id) == before
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
