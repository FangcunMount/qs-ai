import asyncio
from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from qs_ai.application.integration.events import DeliverResults
from qs_ai.application.interpretation.commands import CancelCommand
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql.result_outbox import MySQLResultOutbox, stage_state
from qs_ai.infrastructure.persistence.mysql.schema import external_requests, jobs, result_outbox
from tests.integration.test_interpretation import kit  # noqa: F401
from tests.test_input_binding import bound_case

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("published_configuration")]


async def test_external_start_is_atomic_and_globally_idempotent(kit, monkeypatch):  # noqa: F811
    import qs_ai.infrastructure.persistence.mysql.interpretation as persistence

    original = persistence.stage_state
    request_id = str(uuid4())

    async def failed_stage(db, session):
        await original(db, session)
        raise RuntimeError("after result staging")

    monkeypatch.setattr(persistence, "stage_state", failed_stage)
    with pytest.raises(RuntimeError):
        await kit.service.start_external(
            kit.actor, "7", ("42",), "goal", request_id, bound_case()[1].items
        )
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(external_requests)
                .where(external_requests.c.request_id == request_id)
            )
            == 0
        )
    monkeypatch.setattr(persistence, "stage_state", original)
    receipts = await asyncio.gather(
        *[
            kit.service.start_external(
                kit.actor, "7", ("42",), "goal", request_id, bound_case()[1].items
            )
            for _ in range(2)
        ]
    )
    assert receipts[0] == receipts[1]
    session_id = receipts[0].session_id
    async with kit.transactions.open() as db:
        for table in (jobs, result_outbox):
            assert (
                await db.scalar(
                    select(func.count()).select_from(table).where(table.c.session_id == session_id)
                )
                == 1
            )
    with pytest.raises(RuleViolation, match="idempotency_conflict"):
        await kit.service.start_external(
            kit.actor, "7", ("42",), "changed", request_id, bound_case()[1].items
        )


async def test_cancel_publishes_new_snapshot_and_failed_delivery_keeps_payload(kit):  # noqa: F811
    receipt = await kit.service.start_external(
        kit.actor, "7", ("42",), "goal", str(uuid4()), bound_case()[1].items
    )
    await kit.worker().once()
    view = await kit.service.get(kit.actor, receipt.session_id)
    await kit.service.cancel(
        kit.actor,
        receipt.session_id,
        CancelCommand(expected_version=view.session.version),
        str(uuid4()),
    )
    store = MySQLResultOutbox(kit.transactions)
    before = sorted(await store.pending(20), key=lambda event: event.version)
    assert [event.status for event in before] == [
        "queued",
        "running",
        "awaiting_answer",
        "cancelled",
    ]
    assert len({event.event_id for event in before}) == len(before)

    class Unavailable:
        async def accept(self, event):
            raise TimeoutError("lost confirmation")

    assert await DeliverResults(store, Unavailable()).once() == 0
    async with kit.transactions.open() as db:
        rows = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.session_id == receipt.session_id)
                )
            )
            .mappings()
            .all()
        )
        assert all(not row["delivered"] and row["attempts"] == 1 for row in rows)
        assert {row["event_id"] for row in rows} == {event.event_id for event in before}


async def test_delivery_timing_survives_restage_retry_and_duplicate_ack(kit):  # noqa: F811
    receipt = await kit.service.start_external(
        kit.actor, "7", ("42",), "goal", str(uuid4()), bound_case()[1].items
    )
    store = MySQLResultOutbox(kit.transactions)
    [event] = await store.pending(20)
    created = datetime(2026, 1, 1)
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == event.event_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["created_at"] is not None and row["delivered_at"] is None
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.event_id == event.event_id)
            .values(created_at=created)
        )
        await stage_state(db, (await kit.service.get(kit.actor, receipt.session_id)).session)
        await db.commit()
    await store.retry(event.event_id)
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == event.event_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["created_at"] == created
        assert row["delivered_at"] is None and not row["delivered"]
        assert row["available_at"] > created and row["attempts"] == 1
    await store.delivered(event.event_id)
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == event.event_id)
                )
            )
            .mappings()
            .one()
        )
        acknowledged = row["delivered_at"]
        assert acknowledged > created and row["delivered"]
    await asyncio.gather(store.delivered(event.event_id), store.retry(event.event_id))
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == event.event_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["created_at"] == created and row["delivered_at"] == acknowledged
        assert row["attempts"] == 1


async def test_replayed_old_delivered_event_does_not_invent_delivery_time(kit):  # noqa: F811
    await kit.service.start_external(
        kit.actor, "7", ("42",), "goal", str(uuid4()), bound_case()[1].items
    )
    store = MySQLResultOutbox(kit.transactions)
    [event] = await store.pending(20)
    async with kit.transactions.open() as db:
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.event_id == event.event_id)
            .values(created_at=None, delivered_at=None, delivered=True)
        )
        await db.commit()
    await store.delivered(event.event_id)
    async with kit.transactions.open() as db:
        row = (
            (
                await db.execute(
                    select(result_outbox).where(result_outbox.c.event_id == event.event_id)
                )
            )
            .mappings()
            .one()
        )
        assert row["created_at"] is None and row["delivered_at"] is None
