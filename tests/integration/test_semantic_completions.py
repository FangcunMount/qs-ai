import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import delete, select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import complete_semantic
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_semantic_completions as table
from tests.integration.test_evaluation_completions import accept, next_prepared, stored
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_semantic_output import context

pytestmark = pytest.mark.integration


@pytest.fixture
async def judge(dispatched):
    tx, run_id, generated, routes, _ = dispatched
    raw = json.dumps(context()[-1], ensure_ascii=False).encode()
    completion = SemanticCompletion(
        "execution:2",
        "candidate:1",
        generated.normalized_fingerprint,
        1,
        "invocation:2",
        "succeeded",
        generated.finished_at,
        generated.finished_at + timedelta(seconds=1),
        1,
        replace(generated.receipt, invocation_id="invocation:2"),
        raw,
        raw,
    )
    async with tx.open() as db:
        await accept(
            db,
            dispatched,
            assertions=(
                AssertionReceipt("schema", "default", 1, True, "deterministic", "passed", "valid"),
                AssertionReceipt(
                    "faithful", "case", 1, True, "semantic", "pending_semantic", "pending"
                ),
            ),
        )
        await next_prepared(db, run_id, generated)
        await reserve_dispatch(db, run_id, 7, "worker:2", generated.finished_at)
        await db.commit()
    try:
        yield tx, run_id, completion, routes
    finally:
        async with tx.open() as db:
            await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()


async def complete(db, judge, **changes):
    _, run_id, completion, routes = judge
    args = dict(
        run_id=run_id,
        expected_version=8,
        organization_id=1,
        owner="worker:2",
        completion=completion,
        routes=routes,
    )
    args.update(changes)
    return await complete_semantic(db, **args)


async def evidence(tx, run_id):
    async with tx.open() as db:
        return (
            (await db.execute(select(table).where(table.c.run_id == str(run_id)))).mappings().all()
        )


async def test_quality_failed_is_review_ready_with_original_generation_preserved(judge):
    tx, run_id, value, _ = judge
    before = (await stored(tx, run_id))[0]
    async with tx.open() as db:
        state = await complete(db, judge)
        await db.commit()
    assert state.version == 9 and state.checkpoint is None
    after = (await stored(tx, run_id))[0]
    assert after["raw_output"] == before["raw_output"]
    assert after["normalized_output"] == before["normalized_output"]
    assert after["candidate_json"]["review_ready"] is True
    assert after["candidate_json"]["assertions"][0] == before["candidate_json"]["assertions"][0]
    assert after["candidate_json"]["assertions"][1]["status"] == "failed"
    (record,) = await evidence(tx, run_id)
    assert record["normalized_output"] == value.normalized_output
    assert record["result_json"]["output_fingerprint"] == value.output_fingerprint
    for version in (8, 9):
        async with tx.open() as db:
            with pytest.raises(CheckpointConflict):
                await complete(db, judge, expected_version=version)


async def test_failure_after_candidate_update_rolls_back_evidence_and_candidate(judge, monkeypatch):
    from qs_ai.infrastructure.persistence.mysql import evaluation_semantic

    tx, run_id, _, _ = judge
    before = await stored(tx, run_id)
    run_before = await rows(tx, run_id)

    async def fail(*args):
        raise RuntimeError("injected checkpoint failure")

    monkeypatch.setattr(evaluation_semantic, "save_checkpoint", fail)
    with pytest.raises(RuntimeError, match="injected"):
        async with tx.open() as db:
            await complete(db, judge)
    assert await evidence(tx, run_id) == []
    assert await stored(tx, run_id) == before
    assert await rows(tx, run_id) == run_before


@pytest.mark.parametrize(
    "field", ["owner", "organization_id", "fingerprint", "candidate", "decision"]
)
async def test_foreign_or_mismatched_semantic_result_cannot_change_candidate(judge, field):
    tx, run_id, value, _ = judge
    changes = {}
    if field == "owner":
        changes["owner"] = "worker:other"
    elif field == "organization_id":
        changes["organization_id"] = 2
    elif field == "fingerprint":
        changes["completion"] = replace(value, candidate_output_fingerprint="sha256:" + "b" * 64)
    elif field == "candidate":
        changes["completion"] = replace(value, candidate_id="candidate:other")
    else:
        output = json.loads(value.normalized_output)
        output["decisions"][0]["type"] = "unrequested"
        changes["completion"] = replace(value, normalized_output=json.dumps(output).encode())
    before = await stored(tx, run_id)
    async with tx.open() as db:
        with pytest.raises((ValueError, CheckpointConflict)):
            await complete(db, judge, **changes)
    assert await evidence(tx, run_id) == []
    assert await stored(tx, run_id) == before


@pytest.mark.parametrize("unknown", [False, True])
async def test_failed_judge_never_marks_candidate_review_ready(judge, unknown):
    from qs_ai.domain.evaluation.failure import ClassifiedFailure

    tx, run_id, value, _ = judge
    failure = ClassifiedFailure(
        "semantic_evaluation",
        "result_unknown" if unknown else "semantic_execution",
        "response_unknown" if unknown else "semantic_output_schema_invalid",
        not unknown,
        unknown,
        "manual_acknowledgement" if unknown else "retry_semantic",
        "Judge execution failed",
        (value.execution_id,),
    )
    value = replace(
        value,
        status="result_unknown" if unknown else "failed",
        failure=failure,
        normalized_output=b"invalid-json",
    )
    async with tx.open() as db:
        await complete(db, judge, completion=value)
        await db.commit()
    assert (await stored(tx, run_id))[0]["candidate_json"]["review_ready"] is False
    (record,) = await evidence(tx, run_id)
    assert record["normalized_output"] == b"invalid-json" and record["result_json"] is None
    run, _, checkpoint = await rows(tx, run_id)
    assert run["progress_json"]["status"] == ("blocked" if unknown else "collecting")
    assert checkpoint["version"] == 9 and checkpoint["checkpoint_json"] is None


async def test_concurrent_semantic_completion_accepts_once(judge):
    import asyncio

    tx, run_id, _, _ = judge

    async def attempt():
        async with tx.open() as db:
            state = await complete(db, judge)
            await db.commit()
            return state

    results = await asyncio.gather(attempt(), attempt(), return_exceptions=True)
    assert sum(isinstance(result, CheckpointConflict) for result in results) == 1
    assert len(await evidence(tx, run_id)) == 1
