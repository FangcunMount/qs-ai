"""Capacity is enforced through actual admission/claim/terminal transactions."""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.interpretation.commands import CancelCommand
from qs_ai.application.interpretation.ports import WorkflowResult
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.domain.interpretation.model import EvidenceItem, Fact
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.schema import jobs, result_outbox, sessions
from qs_ai.infrastructure.persistence.mysql.schema import (
    participant_capacity_reservations as ledger,
)
from tests.integration.test_interpretation import expire
from tests.integration.test_interpretation import kit as kit

pytestmark = pytest.mark.integration


def services(kit, policy):
    return (
        InterpretationService(MySQLUnitOfWorkFactory(kit.transactions, policy), kit.source),
        MySQLExecutionStore(kit.transactions, policy),
    )


async def start(kit, service, request_id=None):
    item = EvidenceItem("42", "7", "99", "v1", (Fact("standard_report", "{}"),))
    return await service.start_external(
        kit.actor, "7", ("42",), "容量验证", request_id or str(uuid4()), (item,)
    )


async def reservations(kit):
    async with kit.transactions.open() as db:
        return (
            (await db.execute(select(ledger).where(ledger.c.subject_id == kit.actor.subject_id)))
            .mappings()
            .all()
        )


@pytest.mark.parametrize("dimension", ["org", "user", "assessment"])
async def test_concurrent_admission_obeys_each_daily_dimension_atomically(kit, dimension):
    policy = replace(ParticipantCapacityPolicy(), **{"daily_" + dimension: 1})
    service, _ = services(kit, policy)
    results = await asyncio.gather(*(start(kit, service) for _ in range(3)), return_exceptions=True)
    assert all(not isinstance(v, Exception) for v in results)
    assert sum(v.status == "blocked" for v in results) == 2
    assert sum(v.status == "queued" for v in results) == 1
    assert len(await reservations(kit)) == 1
    async with kit.transactions.open() as db:
        assert (
            await db.execute(
                select(func.count())
                .select_from(sessions)
                .where(sessions.c.owner_subject_id == kit.actor.subject_id)
            )
        ).scalar_one() == 3
        payloads = (
            (
                await db.execute(
                    select(result_outbox.c.payload).where(
                        result_outbox.c.session_id.in_([v.session_id for v in results])
                    )
                )
            )
            .scalars()
            .all()
        )
        assert (
            sum(p["failure_code"] == "participant_daily_capacity_exceeded" for p in payloads) == 2
        )
        assert (
            await db.execute(
                select(func.count())
                .select_from(jobs)
                .where(jobs.c.session_id.in_([v.session_id for v in results]))
            )
        ).scalar_one() == 1


async def test_original_request_replay_reserves_one_call_and_cancel_does_not_refund(kit):
    service, _ = services(kit, ParticipantCapacityPolicy(daily_user=1))
    key = str(uuid4())
    original = await start(kit, service, key)
    assert await start(kit, service, key) == original
    await service.cancel(kit.actor, original.session_id, CancelCommand(original.version), "cancel")
    saved = await reservations(kit)
    assert len(saved) == 1 and not saved[0]["active"]
    denied_key = str(uuid4())
    denied = await start(kit, service, denied_key)
    assert denied.status == "blocked"
    assert await start(kit, service, denied_key) == denied
    assert await reservations(kit) == saved


@pytest.mark.parametrize("dimension", ["org", "user", "assessment"])
async def test_active_limit_defers_work_and_lease_recovery_reuses_slot(kit, dimension):
    policy = replace(
        ParticipantCapacityPolicy(active_org=3, active_user=3, active_assessment=3),
        **{"active_" + dimension: 1},
    )
    service, store = services(kit, policy)
    first = await start(kit, service)
    second = await start(kit, service)
    claim = await store.claim(60)
    assert claim.session.id == first.session_id
    assert await store.claim(60) is None
    queued = await service.get(kit.actor, second.session_id)
    assert queued.session.status == "queued"
    before = await reservations(kit)
    assert sum(r["active"] for r in before) == 1
    await expire(kit, first.session_id)
    resumed = await store.claim(60)
    assert resumed.run_id == claim.run_id
    assert await reservations(kit) == before
    await store.finish(resumed, WorkflowResult("", failure_code="model_not_connected"))
    assert not any(r["active"] for r in await reservations(kit))
    async with kit.transactions.open() as db:
        await db.execute(
            update(jobs)
            .where(jobs.c.session_id == second.session_id)
            .values(available_at=func.utc_timestamp(6))
        )
        await db.commit()
    next_claim = await store.claim(60)
    assert next_claim.session.id == second.session_id
    assert sum(r["active"] for r in await reservations(kit)) == 1
    # A current cancellation releases the slot but preserves both daily receipts.
    await service.cancel(
        kit.actor, second.session_id, CancelCommand(next_claim.session.version), "cancel"
    )
    saved = await reservations(kit)
    assert len(saved) == 2 and not any(r["active"] for r in saved)
