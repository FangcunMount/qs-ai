from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import delete, select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import complete_generation
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import (
    execute_preflight,
    transition_requested,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_generation_completions as table
from tests.integration.test_evaluation_runs import create, rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_generation_completion_assets import assets

pytestmark = pytest.mark.integration


@pytest.fixture
async def dispatched(setup_run):
    tx, run_id, release = setup_run
    bound, value, routes, schemas = assets()
    release = replace(
        release, generation_route=bound.generation_route, output_schema=bound.output_schema
    )
    value = replace(value, case_id="PROMPT-EVAL-001")
    at = value.started_at
    async with tx.open() as db:
        await create(db, run_id, release)
        await transition_requested(db, run_id, 1, 1, "collecting", "actor:1", "开始", at)
        await execute_preflight(db, run_id, 2, 1, at)
        await prepare_execution(
            db,
            run_id,
            3,
            1,
            "worker:1",
            value.execution_id,
            value.invocation_id,
            at,
            at + timedelta(seconds=30),
        )
        await reserve_dispatch(db, run_id, 4, "worker:1", at)
        await db.commit()
    try:
        yield tx, run_id, value, routes, schemas
    finally:
        async with tx.open() as db:
            await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()


async def accept(db, context, **changes):
    _, run_id, value, routes, schemas = context
    args = dict(
        run_id=run_id,
        expected_version=5,
        organization_id=1,
        owner="worker:1",
        completion=value,
        routes=routes,
        schemas=schemas,
        candidate_id="candidate:1",
        assertions=(
            AssertionReceipt(
                "schema", "default", 1, True, "deterministic", "passed", "Schema valid"
            ),
        ),
    )
    args.update(changes)
    return await complete_generation(db, **args)


async def stored(tx, run_id):
    async with tx.open() as db:
        return (
            (await db.execute(select(table).where(table.c.run_id == str(run_id)))).mappings().all()
        )


async def test_success_atomically_preserves_bytes_candidate_and_version(dispatched):
    tx, run_id, value, _, _ = dispatched
    async with tx.open() as db:
        state = await accept(db, dispatched)
        await db.commit()
    assert state.version == 6 and state.checkpoint is None
    (evidence,) = await stored(tx, run_id)
    assert evidence["normalized_output"] == value.normalized_output
    assert evidence["raw_output"] == value.raw_output
    assert evidence["candidate_json"]["generation_execution_id"] == value.execution_id
    assert evidence["candidate_json"]["review_ready"] is False
    for version in (5, 6):
        async with tx.open() as db:
            with pytest.raises(CheckpointConflict):
                await accept(db, dispatched, expected_version=version)
    assert len(await stored(tx, run_id)) == 1


async def test_caller_rollback_restores_checkpoint_and_removes_evidence(dispatched):
    tx, run_id, _, _, _ = dispatched
    before = await rows(tx, run_id)
    async with tx.open() as db:
        await accept(db, dispatched)
        await db.rollback()
    assert await rows(tx, run_id) == before
    assert await stored(tx, run_id) == []
    async with tx.open() as db:
        await accept(db, dispatched)
        await db.commit()
    assert len(await stored(tx, run_id)) == 1


@pytest.mark.parametrize(
    "change", [{"owner": "worker:other"}, {"organization_id": 2}, {"expected_version": 4}]
)
async def test_stale_or_foreign_receipt_cannot_change_run(dispatched, change):
    tx, run_id, _, _, _ = dispatched
    before = await rows(tx, run_id)
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await accept(db, dispatched, **change)
    assert await rows(tx, run_id) == before
    assert await stored(tx, run_id) == []


async def test_unknown_blocks_run_without_accepting_candidate(dispatched):
    tx, run_id, value, _, _ = dispatched
    failure = ClassifiedFailure(
        "generation_execution",
        "result_unknown",
        "response_unknown",
        False,
        True,
        "manual_acknowledgement",
        "Response unknown",
        (value.execution_id,),
    )
    value = replace(value, status="result_unknown", failure=failure)
    async with tx.open() as db:
        with pytest.raises(ValueError, match="cannot create candidate"):
            await accept(db, dispatched, completion=value)
        await accept(db, dispatched, completion=value, candidate_id="", assertions=())
        await db.commit()
    run, _, checkpoint = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    assert run["progress_json"]["unresolved_result_unknown_count"] == 1
    assert checkpoint["version"] == 6 and checkpoint["checkpoint_json"] is None
    (evidence,) = await stored(tx, run_id)
    assert evidence["candidate_id"] is None and evidence["candidate_json"] is None


async def test_competing_completions_accept_exactly_one_candidate(dispatched):
    import asyncio

    tx, run_id, _, _, _ = dispatched

    async def attempt(candidate_id):
        async with tx.open() as db:
            result = await accept(db, dispatched, candidate_id=candidate_id)
            await db.commit()
            return result

    results = await asyncio.gather(
        attempt("candidate:1"), attempt("candidate:2"), return_exceptions=True
    )
    assert sum(isinstance(r, CheckpointConflict) for r in results) == 1
    records = await stored(tx, run_id)
    assert len(records) == 1
    assert records[0]["candidate_id"] in ("candidate:1", "candidate:2")


async def test_exception_after_evidence_insert_rolls_back_all_writes(dispatched, monkeypatch):
    from qs_ai.infrastructure.persistence.mysql import evaluation_completions

    tx, run_id, _, _, _ = dispatched
    before = await rows(tx, run_id)

    async def fail(*args):
        raise RuntimeError("injected version write failure")

    monkeypatch.setattr(evaluation_completions, "save_checkpoint", fail)
    with pytest.raises(RuntimeError, match="injected"):
        async with tx.open() as db:
            await accept(db, dispatched)
    assert await rows(tx, run_id) == before
    assert await stored(tx, run_id) == []


async def test_route_mismatch_does_not_store_success(dispatched):
    tx, run_id, value, _, _ = dispatched
    value = replace(value, receipt=replace(value.receipt, model="other"))
    async with tx.open() as db:
        with pytest.raises(ValueError, match="receipt"):
            await accept(db, dispatched, completion=value)
    assert await stored(tx, run_id) == []
    assert (await rows(tx, run_id))[2]["version"] == 5
