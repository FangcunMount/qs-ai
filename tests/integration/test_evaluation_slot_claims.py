import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import delete, insert, update
from sqlalchemy.exc import IntegrityError

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    read_response,
    save_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    active_claims,
    create_claim,
    lock_run,
    release_claim,
    renew_claim,
    require_claim,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_response_receipts,
    evaluation_runs,
    evaluation_slot_claims,
)
from tests.integration.test_evaluation_dispatches import prepared  # noqa: F401

pytestmark = pytest.mark.integration


@pytest.fixture
async def candidate_run(prepared):  # noqa: F811
    tx, _, run_id, cp = prepared
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(execution_mode="candidate_v2")
        )
        await db.execute(
            update(evaluation_checkpoints)
            .where(evaluation_checkpoints.c.run_id == str(run_id))
            .values(checkpoint_json=None)
        )
        await db.commit()
    try:
        yield tx, run_id, cp
    finally:
        async with tx.open() as db:
            for table in (evaluation_response_receipts, evaluation_slot_claims):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()


async def test_only_one_owner_can_claim_slot(candidate_run):
    tx, run_id, cp = candidate_run

    async def claim(owner):
        async with tx.open() as db:
            await lock_run(db, run_id, 1)
            value = await create_claim(db, run_id, replace(cp, owner=owner), 2)
            await db.commit()
            return value

    results = await asyncio.gather(claim("owner:1"), claim("owner:2"), return_exceptions=True)
    assert sum(isinstance(r, IntegrityError) for r in results) == 1
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        assert len(await active_claims(db, run_id)) == 1
        with pytest.raises(CheckpointConflict):
            await lock_run(db, run_id, 2)


async def test_renewal_and_slot_reuse_fence_old_owner(candidate_run):
    tx, run_id, cp = candidate_run
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        old = await create_claim(db, run_id, cp, 2)
        await db.commit()
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        renewed = await renew_claim(
            db,
            old,
            cp.claimed_at + timedelta(seconds=10),
            cp.lease_expires_at + timedelta(seconds=30),
        )
        assert (await require_claim(db, old)) == renewed
        await release_claim(db, renewed)
        new = await create_claim(db, run_id, replace(cp, owner="owner:2"), 3)
        await db.commit()
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        with pytest.raises(CheckpointConflict):
            await release_claim(db, old)
        assert await require_claim(db, new) == new


async def test_persisted_response_survives_failed_projection_and_is_immutable(candidate_run):
    tx, run_id, cp = candidate_run
    cp = cp.mark_dispatching(cp.owner, cp.claimed_at)
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        claim = await create_claim(db, run_id, cp, 2)
        await db.execute(
            insert(evaluation_dispatches).values(
                run_id=str(run_id),
                invocation_id=cp.invocation_id,
                execution_id=cp.execution_id,
                kind=cp.kind,
                case_id=cp.case_id,
                slot_ordinal=cp.slot_ordinal,
                candidate_id=cp.candidate_id,
                checkpoint_json=encode(cp),
            )
        )
        await db.commit()
    finished = cp.claimed_at + timedelta(seconds=1)
    response = ModelResponse(cp.invocation_id, "request:1", "model", "{}", "{}", "", None, None, 10)
    result = EvaluationResponse(response, None, finished)
    async with tx.open() as db:
        await save_response(db, 1, claim, result, at=finished)
        await db.commit()
    with pytest.raises(RuntimeError):
        async with tx.open() as db:
            await lock_run(db, run_id, 1)
            await release_claim(db, claim)
            raise RuntimeError("projection crashed")
    async with tx.open() as db:
        assert await read_response(db, claim) == result
        await save_response(db, 1, claim, result, at=finished)
        with pytest.raises(CheckpointConflict, match="Immutable"):
            await save_response(
                db,
                1,
                claim,
                replace(result, finished_at=finished + timedelta(seconds=1)),
                at=finished + timedelta(seconds=1),
            )
        await db.commit()


async def test_late_commit_cannot_backdate_response_to_bypass_expiry(candidate_run):
    tx, run_id, cp = candidate_run
    cp = cp.mark_dispatching(cp.owner, cp.claimed_at)
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        claim = await create_claim(db, run_id, cp, 2)
        await db.execute(
            insert(evaluation_dispatches).values(
                run_id=str(run_id),
                invocation_id=cp.invocation_id,
                execution_id=cp.execution_id,
                kind=cp.kind,
                case_id=cp.case_id,
                slot_ordinal=cp.slot_ordinal,
                candidate_id=cp.candidate_id,
                checkpoint_json=encode(cp),
            )
        )
        await db.commit()
    result = EvaluationResponse(
        ModelResponse(cp.invocation_id, "req:1", "model", "{}", "{}", "", None, None, 1),
        None,
        cp.claimed_at,
    )
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict, match="expiry"):
            await save_response(db, 1, claim, result, at=cp.lease_expires_at)
        assert await read_response(db, claim) is None
