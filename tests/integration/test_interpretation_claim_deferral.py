"""Real job-only quota deferral; no message, witness commit or model claim."""

from uuid import uuid4

import pytest
from reliable_messaging.transaction import Decision, StoreChecker
from sqlalchemy import select, update

from qs_ai.infrastructure.persistence.mysql.database import EventSession
from qs_ai.infrastructure.persistence.mysql.schema import (
    participant_capacity_reservations,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.interpretation_claim_deferral import (
    InterpretationClaimDeferral,
    InterpretationDeferralPending,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_claim_root import rows
from tests.integration.test_transaction_interpretation_claim import candidate
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys

pytestmark = pytest.mark.integration


async def prior(host, *, denied=True, abort=True):
    if denied:
        first, _, _ = await candidate(host, snapshot=True)
        claim = await host.kit.store.claim(60)
        assert claim is not None and claim.session.id == first.session.id
    source, before, plan = await candidate(host, snapshot=True)
    owned = await host.store.reserve(source.intent(60).request, plan)
    assert owned.acquired
    host.executions.append(owned.execution)
    if abort:
        assert await host.store.abort(owned.execution)
    deferred = InterpretationClaimDeferral(host.kit.store, host.store, host.archives)
    return source, before, owned.execution, deferred


@pytest.mark.parametrize("lost_reply", [False, True])
async def test_deferral_keeps_job_identity_attempt_state_and_old_abort_without_model_lease(
    host,
    monkeypatch,
    lost_reply,
):
    source, before, execution, deferred = await prior(host)
    with monkeypatch.context() as patch:
        if lost_reply:
            commit = EventSession.commit

            async def discarded(db):
                await commit(db)
                raise ConnectionError("client discarded completed job-only deferral reply")

            patch.setattr(EventSession, "commit", discarded)
        if lost_reply:
            with pytest.raises(InterpretationDeferralPending):
                await deferred(source, 60, execution)
        else:
            assert await deferred(source, 60, execution) is True
    after = await rows(host.kit, source.session.id)
    assert after[0] == before[0] and after[2:] == before[2:]
    old_job, new_job = dict(before[1][0]), dict(after[1][0])
    old_at, new_at = old_job.pop("available_at"), new_job.pop("available_at")
    assert new_at > old_at and new_job == old_job
    assert not after[3] and not after[4]  # No model lease, first event or pending publisher.
    assert await StoreChecker(host.store).check(execution.references[0]) == Decision.ROLLBACK
    async with host.tx.open() as db:
        reservation = (
            (
                await db.execute(
                    select(participant_capacity_reservations).where(
                        participant_capacity_reservations.c.run_id == source.session.active_run_id,
                    )
                )
            )
            .mappings()
            .one()
        )
        assert not reservation["active"] and reservation["released_at"] is None


async def test_capacity_becoming_available_cannot_commit_a_slot_or_model_handoff(host):
    source, before, execution, deferred = await prior(host, denied=False)
    assert await deferred(source, 60, execution) is False
    assert await rows(host.kit, source.session.id) == before
    async with host.tx.open() as db:
        assert (
            await db.scalar(
                select(participant_capacity_reservations.c.run_id).where(
                    participant_capacity_reservations.c.run_id == source.session.active_run_id,
                )
            )
            is None
        )
    assert await StoreChecker(host.store).check(execution.references[0]) == Decision.ROLLBACK


async def test_prepared_publication_is_not_permission_to_defer_business(host):
    source, before, execution, deferred = await prior(host, abort=False)
    with pytest.raises(InterpretationDeferralPending):
        await deferred(source, 60, execution)
    assert await rows(host.kit, source.session.id) == before
    assert await StoreChecker(host.store).check(execution.references[0]) == Decision.UNKNOWN


async def test_original_job_scope_change_prevents_operational_deferral(host):
    source, _, execution, deferred = await prior(host)
    async with host.tx.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == source.session.id)
            .values(
                request_id=str(uuid4()),
            )
        )
        await db.commit()
    competing = await rows(host.kit, source.session.id)
    assert await deferred(source, 60, execution) is False
    assert await rows(host.kit, source.session.id) == competing


async def test_new_writer_epoch_cannot_use_old_abort_to_write_original_job(host):
    from reliable_messaging.transaction import BindingError

    from qs_ai.infrastructure.persistence.mysql.transaction_schema import binding

    source, before, execution, deferred = await prior(host)
    async with host.tx.open() as db:
        await db.execute(
            update(binding)
            .where(binding.c.owner == b"ai.mysql")
            .values(
                writer_epoch=b"another-epoch",
            )
        )
        await db.commit()
    try:
        with pytest.raises(BindingError):
            await deferred(source, 60, execution)
        assert await rows(host.kit, source.session.id) == before
    finally:
        async with host.tx.open() as db:
            await db.execute(
                update(binding)
                .where(binding.c.owner == b"ai.mysql")
                .values(
                    writer_epoch=host.store.binding.writer_epoch.encode(),
                )
            )
            await db.commit()


async def test_job_only_commit_replaces_only_its_root_recorder_and_never_calls_legacy(host):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    source, before, execution, deferred = await prior(host)
    old = host.tx.database.state_events
    legacy = SimpleNamespace(flush=AsyncMock(side_effect=AssertionError("Legacy flush prohibited")))
    host.tx.database.state_events = legacy
    try:
        assert await deferred(source, 60, execution) is True
        assert host.tx.database.state_events is legacy
        legacy.flush.assert_not_called()
    finally:
        host.tx.database.state_events = old
    after = await rows(host.kit, source.session.id)
    assert after[0] == before[0] and after[2:] == before[2:]
    assert after[1][0]["available_at"] > before[1][0]["available_at"]
    assert await StoreChecker(host.store).check(execution.references[0]) == Decision.ROLLBACK


@pytest.mark.parametrize("damage", ["interpretation", "evaluation"])
async def test_operational_root_rolls_back_any_accidental_state_mutation(host, monkeypatch, damage):
    from dataclasses import replace

    from reliable_messaging.transaction import BindingError

    from qs_ai.infrastructure.persistence.mysql.database import changed_evaluation
    from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork
    from qs_ai.infrastructure.workflow_transport import interpretation_claim_deferral as module

    source, before, execution, deferred = await prior(host)

    async def invalid(db, *_):
        if damage == "interpretation":
            value = replace(source.session)
            value.running()
            await MySQLUnitOfWork(db).save(value)
        else:
            changed_evaluation(db, "unexpected-evaluation-write")
        return False

    monkeypatch.setattr(module, "acquire", invalid)
    with pytest.raises(BindingError):
        await deferred(source, 60, execution)
    assert await rows(host.kit, source.session.id) == before
    assert await StoreChecker(host.store).check(execution.references[0]) == Decision.ROLLBACK
