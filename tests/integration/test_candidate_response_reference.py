"""Real original receipt survives renewable ownership and claim release without new model work."""

from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import insert

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import completion_owner
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    read_response,
    save_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    create_claim,
    lock_run,
    release_claim,
    renew_claim,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_dispatches
from tests.integration.test_evaluation_dispatches import prepared as prepared
from tests.integration.test_evaluation_slot_claims import candidate_run as candidate_run

pytestmark = pytest.mark.integration


@pytest.fixture
async def persisted(candidate_run):
    tx, run_id, original = candidate_run
    cp = original.mark_dispatching(original.owner, original.claimed_at)
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
    async with tx.open() as db:
        assert await read_candidate_response(db, 1, claim) is None
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        renewed = await renew_claim(
            db,
            claim,
            cp.claimed_at + timedelta(seconds=1),
            cp.lease_expires_at + timedelta(seconds=30),
        )
        await db.commit()
    finished = cp.lease_expires_at + timedelta(seconds=1)
    response = ModelResponse(
        cp.invocation_id,
        "request:first",
        "model",
        '{"first":true}',
        '{"first":true}',
        "",
        None,
        None,
        10,
    )
    result = EvaluationResponse(response, None, finished)
    async with tx.open() as db:
        await save_response(db, 1, renewed, result, at=finished)
        await db.commit()
    return tx, run_id, claim, renewed, result


@pytest.mark.parametrize("released", [False, True])
async def test_first_response_and_dispatch_identity_survive_original_lease_renewal_or_release(
    persisted, released
):
    tx, run_id, claim, renewed, result = persisted
    if released:
        async with tx.open() as db:
            await lock_run(db, run_id, 1)
            await release_claim(db, renewed)
            await db.commit()
    async with tx.open() as db:
        first = await read_candidate_response(db, 1, claim)
        current = await read_candidate_response(db, 1, renewed)
        assert await read_response(db, claim) == result
    assert first == current and first.result == result
    assert first.reference.checkpoint == claim.checkpoint
    assert first.result.finished_at > first.reference.checkpoint.lease_expires_at
    if released:
        async with tx.open() as db:
            with pytest.raises(CheckpointConflict):
                await completion_owner(db, run_id, 1, claim)
    else:
        async with tx.open() as db:
            _, _, live = await completion_owner(db, run_id, 1, claim)
            assert live == renewed


async def test_response_reference_cannot_cross_organization_or_original_claim_fence(persisted):
    tx, _, claim, renewed, result = persisted
    for organization, wrong in ((2, claim), (1, replace(claim, version=3))):
        async with tx.open() as db:
            with pytest.raises(CheckpointConflict):
                await read_candidate_response(db, organization, wrong)
    async with tx.open() as db:
        actual = await read_candidate_response(db, 1, renewed)
    assert actual.result == result and actual.reference.version == claim.version
