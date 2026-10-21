"""Actual host claim root ownership; no half/Broker or complete transaction Sender claim."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.transaction import BindingError
from sqlalchemy import select, update

from qs_ai.infrastructure.persistence.mysql import result_outbox as history_module
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_leases,
    jobs,
    result_outbox,
    runs,
    sessions,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


async def rows(kit, session_id):
    async with kit.transactions.open() as db:
        return [
            list((await db.execute(statement)).mappings())
            for statement in (
                select(sessions).where(sessions.c.id == session_id),
                select(jobs).where(jobs.c.session_id == session_id),
                select(runs).where(runs.c.session_id == session_id),
                select(execution_leases).where(execution_leases.c.thread_id == session_id),
                select(result_outbox).where(result_outbox.c.session_id == session_id),
            )
        ]


@pytest.fixture
async def original(kit):
    receipt = await kit.queued()
    request_id = str(uuid4())
    async with kit.transactions.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == receipt.session_id)
            .values(request_id=request_id)
        )
        await db.commit()
        job_id = await db.scalar(select(jobs.c.id).where(jobs.c.session_id == receipt.session_id))
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("exhausted", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_claim_business_lease_attempt_and_first_history_share_original_root(
    original, exhausted, commit, monkeypatch
):
    p = original
    if exhausted:
        async with p.kit.transactions.open() as db:
            await db.execute(update(jobs).where(jobs.c.id == p.job_id).values(attempt=3))
            await db.commit()
    before = await rows(p.kit, p.receipt.session_id)
    event_id = str(uuid4())

    def no_uuid():
        raise AssertionError("First claim event identity must be supplied before this root")

    with monkeypatch.context() as patch:
        patch.setattr(history_module, "uuid4", no_uuid)
        async with p.kit.transactions.open() as db:
            await db.begin()
            root = db.get_transaction()
            staged = await p.kit.store.claim_in(
                db, p.job_id, p.receipt.session_id, 60, state_event_id=event_id
            )
            assert db.get_transaction() is root and root.is_active
            assert staged.status == ("exhausted" if exhausted else "claimed")
            if not exhausted:
                assert (
                    staged.claim.fence == 1
                    and staged.claim.session.version == p.receipt.version + 1
                )
                await p.kit.store._guard(db, staged.claim)
            assert await rows(p.kit, p.receipt.session_id) == before
            if commit:
                await db.commit()
    after = await rows(p.kit, p.receipt.session_id)
    if not commit:
        assert after == before
    else:
        assert after[0][0]["version"] == p.receipt.version + (2 if exhausted else 1)
        assert after[0][0]["status"] == ("blocked" if exhausted else "running")
        assert after[1][0]["status"] == ("dead" if exhausted else "leased")
        assert after[1][0]["attempt"] == (3 if exhausted else 1)
        assert after[2][0]["status"] == ("blocked" if exhausted else "running")
        assert len(after[4]) == 1 and after[4][0]["event_id"] == event_id
        assert after[4][0]["mq_owned"] is False  # Original history only, not SDK publication proof.


@pytest.mark.parametrize("nested", [False, True])
async def test_claim_rejects_inactive_or_nested_original_session_without_writes(original, nested):
    p = original
    before = await rows(p.kit, p.receipt.session_id)
    async with p.kit.transactions.open() as db:
        if nested:
            async with db.begin(), db.begin_nested():
                with pytest.raises(BindingError):
                    await p.kit.store.claim_in(db, p.job_id, p.receipt.session_id, 60)
        else:
            assert db.get_transaction() is None
            with pytest.raises(BindingError):
                await p.kit.store.claim_in(db, p.job_id, p.receipt.session_id, 60)
    assert await rows(p.kit, p.receipt.session_id) == before


@pytest.mark.parametrize("commit", [False, True])
async def test_concurrent_claim_skips_original_locked_session_and_cannot_gain_a_second_lease(
    original, commit
):
    p = original
    before = await rows(p.kit, p.receipt.session_id)
    async with p.kit.transactions.open() as first:
        await first.begin()
        staged = await p.kit.store.claim_in(
            first, p.job_id, p.receipt.session_id, 60, state_event_id=str(uuid4())
        )
        assert staged.status == "claimed" and staged.claim.fence == 1
        async with p.kit.transactions.open() as second:
            await second.begin()
            other = await p.kit.store.claim_in(second, p.job_id, p.receipt.session_id, 60)
            assert other.status == "miss" and other.claim is None
        assert await rows(p.kit, p.receipt.session_id) == before
        if commit:
            await first.commit()
    async with p.kit.transactions.open() as later:
        await later.begin()
        next_stage = await p.kit.store.claim_in(
            later, p.job_id, p.receipt.session_id, 60, state_event_id=str(uuid4())
        )
        assert next_stage.status == ("miss" if commit else "claimed")
        if not commit:
            assert next_stage.claim.fence == 1  # Rolled-back reservation did not consume a fence.
