"""Candidate readonly observation preserves scoped ownership and original root protection."""

from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import select, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import completion_owner
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    active_claims,
    create_claim,
    lock_run,
    release_claim,
    renew_claim,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from tests.integration.test_evaluation_dispatches import prepared as prepared
from tests.integration.test_evaluation_slot_claims import candidate_run as candidate_run

pytestmark = pytest.mark.integration


async def claim_first(fixture):
    tx, run_id, cp = fixture
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        claim = await create_claim(db, run_id, cp, 2)
        await db.commit()
    return claim


@pytest.mark.parametrize("commit", [False, True])
async def test_readonly_candidate_source_keeps_scope_and_original_lease_root_commit_rollback(
    candidate_run, commit
):
    tx, run_id, cp = candidate_run
    claim = await claim_first(candidate_run)
    async with tx.open() as db:
        observed_run, observed_version, observed = await completion_owner(
            db, run_id, 1, claim, lock=False
        )
    assert observed == claim and observed_version == 1
    assert observed_run["execution_mode"] == "candidate_v2"
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await completion_owner(db, run_id, 2, claim, lock=False)
    async with tx.open() as db:
        assert await completion_owner(db, run_id, 1, claim) == (
            observed_run,
            observed_version,
            observed,
        )
        await renew_claim(
            db,
            claim,
            cp.claimed_at + timedelta(seconds=1),
            cp.lease_expires_at + timedelta(seconds=5),
        )
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(progress_json={**observed_run["progress_json"], "status": "blocked"})
        )
        if commit:
            await db.commit()
        else:
            await db.rollback()
    async with tx.open() as db:
        run, version, current = await completion_owner(db, run_id, 1, claim, lock=False)
    assert version == observed_version
    assert run["progress_json"]["status"] == ("blocked" if commit else "collecting")
    assert current.checkpoint.lease_expires_at == cp.lease_expires_at + timedelta(
        seconds=5 if commit else 0
    )


@pytest.mark.parametrize("replacement", ["released", "new_owner"])
async def test_prior_candidate_observation_does_not_survive_original_claim_release_or_reuse(
    candidate_run, replacement
):
    tx, run_id, cp = candidate_run
    claim = await claim_first(candidate_run)
    async with tx.open() as db:
        await completion_owner(db, run_id, 1, claim, lock=False)
    async with tx.open() as db:
        await lock_run(db, run_id, 1)
        await release_claim(db, claim)
        if replacement == "new_owner":
            await create_claim(db, run_id, replace(cp, owner="worker:replacement"), 3)
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await completion_owner(db, run_id, 1, claim)
    async with tx.open() as db:
        active = await active_claims(db, run_id, lock=False)
        progress = (
            await db.execute(
                select(evaluation_runs.c.progress_json).where(
                    evaluation_runs.c.run_id == str(run_id)
                )
            )
        ).scalar_one()
    assert len(active) == (1 if replacement == "new_owner" else 0)
    assert progress["status"] == "collecting"
    if active:
        assert active[0].version == 3 and active[0].checkpoint.owner == "worker:replacement"
