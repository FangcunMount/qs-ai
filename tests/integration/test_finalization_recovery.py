"""Actual host/root recovery, controlled preparation refusal; not network fault proof."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Decision, Request, State, StoreChecker
from sqlalchemy import delete, select, update

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.persistence.mysql.interpretation_finalization_claim import (
    claim_finalization,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_leases,
    model_calls,
    participant_capacity_reservations,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import read_claim_source
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishPending,
)
from tests.integration.test_finalization_characterization import (
    first_terminal_refusal,
    terminal_snapshot,
)
from tests.integration.test_finalization_characterization import (
    kit as kit,
)
from tests.integration.test_finalization_characterization import (
    original_worker as original_worker,
)
from tests.integration.test_interpretation import expire
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import keys as keys
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


async def test_host_same_lease_finalizes_exact_aborted_plan_without_model_replay(original_worker):
    p = original_worker
    before, previous = await first_terminal_refusal(p)
    claim, result = p.finish.await_args.args
    await p.adapter.finish(claim, result)
    after = await terminal_snapshot(p)
    old, new = p.runtime.terminal_attempts
    assert old.references == previous.execution.references and new.plan == old.plan
    assert new.fence == old.fence + 1
    assert after["artifact"] == result.artifact.id
    assert after["job"]["status"] == "done"
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"]["version"] == before["session"]["version"] + 1
    assert after["call"] == before["call"] and p.gateway.calls == 1
    checker = StoreChecker(p.runtime.store)
    assert await checker.check(old.references[0]) == Decision.ROLLBACK
    assert await checker.check(new.references[0]) == Decision.COMMIT
    plan = old.plan
    request = Request(plan.owner, plan.scope, plan.idempotency_key, plan.input_hash)
    assert (await p.runtime.store.lookup(request)).state == State.COMMITTED
    await p.adapter.finish(claim, result)
    assert len(p.runtime.terminal_attempts) == 2 and p.gateway.calls == 1
    assert await terminal_snapshot(p) == after


async def test_concurrent_finalization_takeover_has_one_winner_and_fences_old_writer(
    original_worker,
):
    p = original_worker
    before, previous = await first_terminal_refusal(p)
    old_claim, result = p.finish.await_args.args
    await expire(p.host.kit, old_claim.session.id)
    claims = await asyncio.gather(p.adapter.claim(60), p.adapter.claim(60))
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    current = winners[0]
    assert current.fence == old_claim.fence + 1
    assert current.session == old_claim.session
    for operation in (
        p.host.kit.store.renew(old_claim, 60),
        p.adapter._finish.retry(old_claim, result, previous.execution),
    ):
        with pytest.raises(LeaseLost):
            await operation
    assert len(p.runtime.terminal_attempts) == 1
    await p.adapter.finish(current, result)
    after = await terminal_snapshot(p)
    assert after["artifact"] == result.artifact.id
    assert after["call"] == before["call"] and p.gateway.calls == 1
    assert after["job"]["attempt"] == before["job"]["attempt"]


async def test_repeated_mq_refusals_never_exhaust_business_attempts_or_repeat_model(
    original_worker,
):
    p = original_worker
    p.runtime.terminal_refusals = 5
    before, previous = await first_terminal_refusal(p)
    first_claim, _ = p.finish.await_args.args
    for _ in range(4):
        await expire(p.host.kit, first_claim.session.id)
        with pytest.raises(TransactionSendError):
            await p.worker.once(60)
        current = await terminal_snapshot(p)
        assert current["job"]["attempt"] == before["job"]["attempt"]
        assert current["session"]["version"] == before["session"]["version"]
        assert current["call"] == before["call"] and p.gateway.calls == 1
    await expire(p.host.kit, first_claim.session.id)
    assert await p.worker.once(60)
    after = await terminal_snapshot(p)
    assert after["artifact"] is not None and after["job"]["status"] == "done"
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert p.gateway.calls == 1
    assert all(e.plan == previous.execution.plan for e in p.runtime.terminal_attempts)


async def test_prepared_terminal_cannot_be_bypassed_after_lease_handover(original_worker):
    p = original_worker
    p.runtime.abort_terminal = False
    before, previous = await first_terminal_refusal(p, state=State.PREPARED)
    claim, _ = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    with pytest.raises(InterpretationFinishPending):
        await p.worker.once(60)
    after = await terminal_snapshot(p)
    assert after["artifact"] is None and p.gateway.calls == 1
    assert after["call"] == before["call"]
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"]["version"] == before["session"]["version"]
    assert len(p.runtime.terminal_attempts) == 1
    assert (
        await StoreChecker(p.runtime.store).check(previous.execution.references[0])
        == Decision.UNKNOWN
    )


@pytest.mark.parametrize("lock", [False, True])
async def test_normal_claim_refuses_a_response_that_committed_after_its_probe(
    original_worker, lock
):
    p = original_worker
    before, _ = await first_terminal_refusal(p)
    claim, _ = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    async with p.host.tx.open() as db:
        with pytest.raises(LeaseLost, match="finalization-only"):
            await read_claim_source(p.host.kit.store, db, claim.job_id, claim.session.id, lock=lock)
    after = await terminal_snapshot(p)
    assert after["call"] == before["call"]
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"] == before["session"]


@pytest.mark.parametrize(
    "damage",
    [
        "empty_response",
        "wrong_fence",
        "active_lease",
        "cancelled",
        "missing_slot",
        "future_receipt_fence",
        "zero_receipt_fence",
    ],
)
async def test_conflicting_recovery_evidence_does_not_grant_ordinary_business_work(
    original_worker, damage
):
    p = original_worker
    before, _ = await first_terminal_refusal(p)
    claim, _ = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    async with p.host.tx.open() as db:
        if damage == "empty_response":
            statement = (
                update(model_calls)
                .where(model_calls.c.run_id == claim.run_id)
                .values(response_json=None)
            )
        elif damage in {"future_receipt_fence", "zero_receipt_fence"}:
            statement = (
                update(model_calls)
                .where(model_calls.c.run_id == claim.run_id)
                .values(fence_token=claim.fence + 1 if damage == "future_receipt_fence" else 0)
            )
        elif damage == "wrong_fence":
            statement = (
                update(execution_leases)
                .where(execution_leases.c.thread_id == claim.session.id)
                .values(fence=claim.fence + 1)
            )
        elif damage == "active_lease":
            from qs_ai.infrastructure.persistence.mysql.execution import expiry

            statement = (
                update(execution_leases)
                .where(execution_leases.c.thread_id == claim.session.id)
                .values(expires_at=expiry(60))
            )
        elif damage == "cancelled":
            statement = (
                update(sessions).where(sessions.c.id == claim.session.id).values(status="cancelled")
            )
        else:
            statement = delete(participant_capacity_reservations).where(
                participant_capacity_reservations.c.run_id == claim.run_id
            )
        await db.execute(statement)
        await db.commit()
    assert await p.adapter.claim(60) is None
    after = await terminal_snapshot(p)
    assert after["artifact"] is None and p.gateway.calls == 1
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"]["version"] == before["session"]["version"]
    assert len(p.runtime.terminal_attempts) == 1


async def test_recovery_keeps_existing_capacity_without_model_admission(
    original_worker, monkeypatch
):
    p = original_worker
    before, _ = await first_terminal_refusal(p)
    claim, _ = p.finish.await_args.args
    async with p.host.tx.open() as db:
        first = (
            (
                await db.execute(
                    select(participant_capacity_reservations).where(
                        participant_capacity_reservations.c.run_id == claim.run_id
                    )
                )
            )
            .mappings()
            .one()
        )
    p.workflow.generation.capacity = AsyncMock()
    p.workflow.generation.capacity.acquire_generation.side_effect = AssertionError(
        "No new provider permit"
    )
    await expire(p.host.kit, claim.session.id)
    current = await p.adapter.claim(60)
    assert current is not None
    async with p.host.tx.open() as db:
        second = (
            (
                await db.execute(
                    select(participant_capacity_reservations).where(
                        participant_capacity_reservations.c.run_id == claim.run_id
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(first) == dict(second)
    await p.worker._execute(current)
    p.workflow.generation.capacity.acquire_generation.assert_not_called()
    assert (await terminal_snapshot(p))["artifact"] is not None and p.gateway.calls == 1


async def test_revoked_authorization_cannot_publish_the_first_artifact_or_repeat_model(
    original_worker,
):
    p = original_worker
    before, previous = await first_terminal_refusal(p)
    claim, _ = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    p.host.kit.source.revoked = True
    with pytest.raises(ValueError, match="different business input"):
        await p.worker.once(60)
    after = await terminal_snapshot(p)
    assert after["artifact"] is None and p.gateway.calls == 1
    assert after["session"]["version"] == before["session"]["version"]
    assert p.finish.await_args.args[1].failure_code == "access_revoked"
    assert (
        await StoreChecker(p.runtime.store).check(previous.execution.references[0])
        == Decision.ROLLBACK
    )


async def test_locked_finalization_root_cannot_be_taken_over(original_worker):
    p = original_worker
    before, _ = await first_terminal_refusal(p)
    claim, _ = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    async with p.host.tx.open() as locked:
        await locked.execute(
            select(sessions.c.id).where(sessions.c.id == claim.session.id).with_for_update()
        )
        assert (
            await claim_finalization(p.host.kit.store, claim.job_id, claim.session.id, 60) is None
        )
    after = await terminal_snapshot(p)
    assert after["call"] == before["call"]
    assert after["job"]["fence_token"] == before["job"]["fence_token"]
    assert after["session"]["version"] == before["session"]["version"]


async def test_lost_lease_commit_reply_returns_no_claim_and_preserves_fencing(
    original_worker, monkeypatch
):
    from qs_ai.infrastructure.persistence.mysql.database import EventSession

    p = original_worker
    before, _ = await first_terminal_refusal(p)
    claim, result = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    original_commit = EventSession.commit

    async def discarded(db):
        await original_commit(db)
        raise ConnectionError("client discarded finalization lease commit reply")

    # Only the explicit lease takeover executes in this patch scope, not an
    # arbitrary nth commit of the worker or business+witness root.
    with monkeypatch.context() as patch:
        patch.setattr(EventSession, "commit", discarded)
        with pytest.raises(ConnectionError):
            await claim_finalization(p.host.kit.store, claim.job_id, claim.session.id, 60)
    assert await p.adapter.claim(60) is None
    after = await terminal_snapshot(p)
    assert after["job"]["fence_token"] == claim.fence + 1
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"]["version"] == before["session"]["version"]
    assert after["artifact"] is None and p.gateway.calls == 1
    await expire(p.host.kit, claim.session.id)
    current = await p.adapter.claim(60)
    assert current is not None and current.fence == claim.fence + 2
    await p.adapter.finish(current, result)
    assert (await terminal_snapshot(p))["artifact"] == result.artifact.id
    assert p.gateway.calls == 1


async def test_locked_claim_recheck_sees_response_beyond_its_earlier_snapshot(original_worker):
    p = original_worker
    before, _ = await first_terminal_refusal(p)
    claim, _ = p.finish.await_args.args
    await expire(p.host.kit, claim.session.id)
    async with p.host.tx.open() as db:
        await db.execute(
            update(model_calls)
            .where(model_calls.c.run_id == claim.run_id)
            .values(status="dispatched", response_json=None)
        )
        await db.commit()
    async with p.host.tx.open() as snapshot:
        await snapshot.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        assert (
            await snapshot.scalar(
                select(model_calls.c.status).where(model_calls.c.run_id == claim.run_id)
            )
            == "dispatched"
        )
        async with p.host.tx.open() as writer:
            await writer.execute(
                update(model_calls)
                .where(model_calls.c.run_id == claim.run_id)
                .values(status="response_received", response_json=before["call"]["response_json"])
            )
            await writer.commit()
        with pytest.raises(LeaseLost, match="finalization-only"):
            await read_claim_source(
                p.host.kit.store, snapshot, claim.job_id, claim.session.id, lock=True
            )
    after = await terminal_snapshot(p)
    assert after["call"] == before["call"]
    assert after["job"]["attempt"] == before["job"]["attempt"]
    assert after["session"]["version"] == before["session"]["version"]
