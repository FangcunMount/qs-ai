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


def case_evidence(case_id):
    from qs_ai.infrastructure.qs_server.evaluation_assertions import (
        SEMANTIC_TYPES,
        assertion_inventory,
    )
    from qs_ai.infrastructure.qs_server.evaluation_suite import V6

    inventory = assertion_inventory(V6, case_id)
    assertions = tuple(
        AssertionReceipt(
            a.type,
            a.scope,
            a.ordinal,
            a.hard,
            "deterministic",
            "pending_semantic" if a.type in SEMANTIC_TYPES else "passed",
            "original",
        )
        for a in inventory
    )
    output = context()[-1]
    output["decisions"] = [
        dict(
            type=a.type, scope=a.scope, ordinal=a.ordinal, status="failed", detail="quality failure"
        )
        for a in assertions
        if a.type in SEMANTIC_TYPES
    ]
    return assertions, json.dumps(output, ensure_ascii=False).encode()


@pytest.fixture
async def judge(dispatched):
    tx, run_id, generated, routes, _ = dispatched
    assertions, raw = case_evidence(generated.case_id)
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
        await accept(db, dispatched, assertions=assertions)
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
    assert after["candidate_json"]["semantic_assertions"][0]["status"] == "failed"
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


async def complete_candidate_set(judge):
    from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
    from tests.test_generation_completion_assets import assets

    tx, run_id, first, routes = judge
    _, generated, _, schemas = assets()
    at = first.finished_at
    async with tx.open() as db:
        state = await complete(db, judge)
        await db.commit()
    for index in range(2, 36):
        async with tx.open() as db:
            state = await prepare_execution(
                db,
                run_id,
                state.version,
                1,
                "worker:2",
                f"generation:{index}",
                f"gen-call:{index}",
                at,
                at + timedelta(seconds=30),
            )
            cp = state.checkpoint
            assert (cp.kind, cp.case_id, cp.slot_ordinal) == (
                "generation",
                f"PROMPT-EVAL-{(index - 1) // 5 + 1:03}",
                (index - 1) % 5 + 1,
            )
            state = await reserve_dispatch(db, run_id, state.version, "worker:2", at)
            generation = replace(
                generated,
                execution_id=cp.execution_id,
                invocation_id=cp.invocation_id,
                case_id=cp.case_id,
                slot_ordinal=cp.slot_ordinal,
                started_at=at,
                finished_at=at + timedelta(seconds=1),
                receipt=replace(generated.receipt, invocation_id=cp.invocation_id),
            )
            obligations, semantic_raw = case_evidence(generation.case_id)
            state = await accept(
                db,
                (tx, run_id, generation, routes, schemas),
                expected_version=state.version,
                owner="worker:2",
                candidate_id=f"candidate:{index}",
                assertions=obligations,
            )
            at += timedelta(seconds=1)
            state = await prepare_execution(
                db,
                run_id,
                state.version,
                1,
                "worker:2",
                f"semantic:{index}",
                f"sem-call:{index}",
                at,
                at + timedelta(seconds=30),
            )
            cp = state.checkpoint
            assert cp.kind == "semantic" and cp.candidate_id == f"candidate:{index}"
            state = await reserve_dispatch(db, run_id, state.version, "worker:2", at)
            semantic = replace(
                first,
                execution_id=cp.execution_id,
                invocation_id=cp.invocation_id,
                candidate_id=cp.candidate_id,
                candidate_output_fingerprint=generation.normalized_fingerprint,
                raw_output=semantic_raw,
                normalized_output=semantic_raw,
                started_at=at,
                finished_at=at + timedelta(seconds=1),
                receipt=replace(first.receipt, invocation_id=cp.invocation_id),
            )
            state = await complete(db, judge, expected_version=state.version, completion=semantic)
            await db.commit()
            at += timedelta(seconds=1)
        run, _, _ = await rows(tx, run_id)
        assert run["progress_json"]["status"] == (
            "awaiting_review" if index == 35 else "collecting"
        )
    assert len(await stored(tx, run_id)) == len(await evidence(tx, run_id)) == 35
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await prepare_execution(
                db,
                run_id,
                state.version,
                1,
                "worker:2",
                "extra:1",
                "extra-call:1",
                at,
                at + timedelta(seconds=30),
            )

    return state


async def test_all_35_candidates_complete_before_awaiting_review(judge):
    await complete_candidate_set(judge)


async def test_semantic_retry_keeps_candidate_and_generation_count(judge):
    from qs_ai.domain.evaluation.failure import ClassifiedFailure
    from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution

    tx, run_id, value, _ = judge
    failure = ClassifiedFailure(
        "semantic_evaluation",
        "semantic_execution",
        "semantic_output_schema_invalid",
        True,
        False,
        "retry_semantic",
        "Invalid judge output",
        (value.execution_id,),
    )
    async with tx.open() as db:
        state = await complete(
            db, judge, completion=replace(value, status="failed", failure=failure)
        )
        state = await prepare_execution(
            db,
            run_id,
            state.version,
            1,
            "worker:2",
            "semantic:retry",
            "call:retry",
            value.finished_at,
            value.finished_at + timedelta(seconds=30),
        )
        assert (
            state.checkpoint.kind,
            state.checkpoint.candidate_id,
            state.checkpoint.execution_ordinal,
        ) == ("semantic", value.candidate_id, 2)
        await reserve_dispatch(db, run_id, state.version, "worker:2", value.finished_at)
        await db.commit()
    assert len(await stored(tx, run_id)) == 1


@pytest.mark.parametrize("damage", ["missing", "bytes", "decision"])
async def test_saved_semantic_evidence_damage_blocks_next_candidate(judge, damage):
    from sqlalchemy import update

    from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution

    tx, run_id, value, _ = judge
    async with tx.open() as db:
        await complete(db, judge)
        if damage == "missing":
            await db.execute(delete(table).where(table.c.run_id == str(run_id)))
        elif damage == "bytes":
            await db.execute(
                update(table).where(table.c.run_id == str(run_id)).values(normalized_output=b"{}")
            )
        else:
            result = (
                await db.execute(select(table.c.result_json).where(table.c.run_id == str(run_id)))
            ).scalar_one()
            result["decisions"][0]["status"] = "passed"
            await db.execute(
                update(table).where(table.c.run_id == str(run_id)).values(result_json=result)
            )
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await prepare_execution(
                db,
                run_id,
                9,
                1,
                "worker:2",
                "next:1",
                "call:next",
                value.finished_at,
                value.finished_at + timedelta(seconds=30),
            )
    assert (await rows(tx, run_id))[2]["version"] == 9


async def test_independent_semantic_pass_cannot_erase_deterministic_failure(judge):
    from sqlalchemy import update

    from qs_ai.infrastructure.persistence.mysql.schema import evaluation_generation_completions

    tx, run_id, value, _ = judge
    candidate = (await stored(tx, run_id))[0]["candidate_json"]
    failed = next(a for a in candidate["assertions"] if a["type"] == "forbidden_claims_absent")
    failed["status"] = "failed"
    failed["detail"] = "original deterministic failure"
    output = json.loads(value.normalized_output)
    next(a for a in output["decisions"] if a["type"] == "forbidden_claims_absent")["status"] = (
        "passed"
    )
    value = replace(value, normalized_output=json.dumps(output).encode())
    async with tx.open() as db:
        await db.execute(
            update(evaluation_generation_completions)
            .where(evaluation_generation_completions.c.run_id == str(run_id))
            .values(candidate_json=candidate)
        )
        await complete(db, judge, completion=value)
        await db.commit()
    saved = (await stored(tx, run_id))[0]["candidate_json"]
    assert (
        next(a for a in saved["assertions"] if a["type"] == "forbidden_claims_absent")["status"]
        == "failed"
    )
    assert (
        next(a for a in saved["semantic_assertions"] if a["type"] == "forbidden_claims_absent")[
            "status"
        ]
        == "passed"
    )
