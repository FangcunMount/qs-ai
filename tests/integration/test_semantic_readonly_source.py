"""Actual scoped semantic source observation preserves the original terminal root."""

import json
from dataclasses import asdict
from datetime import timedelta

import pytest

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import (
    load_serial_semantic_state,
    project_semantic_progress,
    read_semantic_acceptance,
)
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_completions import stored
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import complete, evidence
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_readonly_semantic_state_and_original_acceptor_keep_cancel_candidate_and_root_atomic(
    judge, cancel, commit
):
    tx, run_id, value, _ = judge
    if cancel:
        await MySQLEvaluationManagement(tx).cancel(
            ManagementScope(run_id, 1, 42),
            8,
            "Drain original semantic terminal",
            value.finished_at + timedelta(seconds=1),
            discard=False,
            confirm=True,
        )
    before, generations = await rows(tx, run_id), await stored(tx, run_id)
    async with tx.open() as db:
        source = await load_serial_semantic_state(db, run_id, 8, 1, lock=False)
        planned = await read_semantic_acceptance(
            db, run_id, 1, "worker:2", value, source.run, source.checkpoint, judge[3], lock=False
        )
        projected = await project_semantic_progress(
            db,
            run_id,
            value,
            "worker:2",
            source.run,
            planned,
            candidate_claim=False,
            lock=False,
            before_insert=True,
        )
        assert planned.candidate["review_ready"] is True
        assert (
            planned.result is not None
            and planned.result.output_fingerprint == value.output_fingerprint
        )
    assert source.version == (9 if cancel else 8)
    assert source.checkpoint.invocation_id == value.invocation_id
    assert await rows(tx, run_id) == before and await stored(tx, run_id) == generations
    assert await evidence(tx, run_id) == []
    async with tx.open() as db:
        state = await complete(db, judge)
        assert state.version == source.version + 1 and state.checkpoint is None
        if commit:
            await db.commit()
        else:
            await db.rollback()
    if commit:
        (record,) = await evidence(tx, run_id)
        (candidate,) = await stored(tx, run_id)
        assert record["execution_id"] == value.execution_id
        assert record["evidence_json"] == json.loads(json.dumps(planned.evidence))
        assert record["result_json"] == json.loads(json.dumps(asdict(planned.result)))
        assert candidate["candidate_json"] == json.loads(json.dumps(planned.candidate))
        assert record["raw_output"] == value.raw_output
        assert candidate["candidate_json"]["review_ready"] is True
        assert candidate["candidate_json"]["accepted_semantic_execution_id"] == value.execution_id
        assert candidate["raw_output"] == generations[0]["raw_output"]
        assert (await rows(tx, run_id))[2]["version"] == source.version + 1
        assert (await rows(tx, run_id))[0]["progress_json"] == json.loads(json.dumps(projected))
    else:
        assert await rows(tx, run_id) == before and await stored(tx, run_id) == generations
        assert await evidence(tx, run_id) == []
