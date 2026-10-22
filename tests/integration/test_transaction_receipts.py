"""Actual MySQL ACK roots; no Broker or real QS consumer acceptance is claimed."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict
from sqlalchemy import delete, select

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.persistence.mysql.transaction_receipts import TransactionMessagingStore
from qs_ai.infrastructure.persistence.mysql.transaction_schema import event_receipts
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.transaction_command import read_admission_plan
from tests.integration import test_interpretation
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import (
    assert_committed_original_root,
    incoming,
    planner,
    reserve,
    rows,
)
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def kit(published_configuration):
    # Participant configurations reference this publication. Their fixture must
    # release those rows before the publication fixture removes its parent rows.
    async for value in test_interpretation.kit.__wrapped__():
        yield value


@pytest.fixture
async def committed(host):
    message = incoming(host)
    candidate = await planner(host).candidate(message)
    owned = (await reserve(host, message, candidate)).execution
    await assert_committed_original_root(host, message, owned, "queued")
    events = read_admission_plan(message, owned.plan)
    store = TransactionMessagingStore(host.archives)
    try:
        yield SimpleNamespace(host=host, events=events, store=store)
    finally:
        async with host.tx.open() as db:
            await db.execute(
                delete(event_receipts).where(
                    event_receipts.c.event_id.in_(
                        [e.envelope().message_id.encode() for e in events]
                    )
                )
            )
            await db.commit()


def ack(p, event, outcome, *, identity=None, digest=None):
    header = event.envelope()
    return prepare(
        pb.EVENT_ACKNOWLEDGEMENT,
        identity or str(uuid4()),
        header.aggregate_key,
        pb.MessagingBody(
            event_acknowledgement=pb.MessagingEventAcknowledgement(
                event_id=header.message_id,
                event_kind=header.kind,
                event_body_sha256=digest or header.body_sha256,
                outcome=outcome,
            )
        ),
        organization_id=str(event.organization_id),
        signing_key=p.host.keys["qs.sign"],
        recipient_key=p.host.keys["ai.encrypt"],
    )


async def accept(p, message, *, commit=True):
    async with p.host.tx.open() as db:
        await db.begin()
        await p.store.confirm_event(db, message.envelope, message.body)
        if commit:
            await db.commit()


async def receipts(p, event):
    return await rows(
        p.host, event_receipts, event_receipts.c.event_id == event.envelope().message_id.encode()
    )


@pytest.mark.parametrize("kind", [pb.COMMAND_RECEIPT, pb.INTERPRETATION_STATE])
async def test_new_business_receipt_and_original_history_share_ack_commit_and_rollback(
    committed, kind
):
    p = committed
    event = next(e for e in p.events if e.envelope().kind == kind)
    message = ack(p, event, pb.MessagingEventAcknowledgement.STORED)
    await accept(p, message, commit=False)
    assert not await receipts(p, event)
    if kind == pb.INTERPRETATION_STATE:
        assert not (
            await rows(
                p.host, result_outbox, result_outbox.c.event_id == event.envelope().message_id
            )
        )[0]["delivered"]
    await accept(p, message)
    first = await receipts(p, event)
    assert len(first) == 1 and first[0]["outcome"] == pb.MessagingEventAcknowledgement.STORED
    await accept(p, message)
    assert await receipts(p, event) == first  # Original receipt time is immutable.
    assert await p.host.archives.read_committed(event.envelope().message_id) == event
    assert not await rows(p.host, outbox, outbox.c.message_id == event.envelope().message_id)
    if kind == pb.INTERPRETATION_STATE:
        original = await rows(
            p.host, result_outbox, result_outbox.c.event_id == event.envelope().message_id
        )
        assert original[0]["delivered"] and original[0]["mq_owned"]
        assert original[0]["attempts"] == 0


async def test_held_and_stored_receipts_are_retained_without_reopening_completed_delivery(
    committed,
):
    p = committed
    event = next(e for e in p.events if e.envelope().kind == pb.INTERPRETATION_STATE)
    held = ack(p, event, pb.MessagingEventAcknowledgement.TECHNICALLY_HELD)
    stored = ack(p, event, pb.MessagingEventAcknowledgement.STORED)
    await accept(p, held)
    original = await rows(
        p.host, result_outbox, result_outbox.c.event_id == event.envelope().message_id
    )
    assert not original[0]["delivered"]
    await accept(p, stored)
    async with p.host.tx.open() as db:
        completed = (
            (
                await db.execute(
                    select(result_outbox).where(
                        result_outbox.c.event_id == event.envelope().message_id
                    )
                )
            )
            .mappings()
            .one()
        )
    await accept(p, held)
    await accept(p, ack(p, event, pb.MessagingEventAcknowledgement.TECHNICALLY_HELD))
    assert len(await receipts(p, event)) == 3
    assert (
        await rows(p.host, result_outbox, result_outbox.c.event_id == event.envelope().message_id)
    )[0] == completed


async def test_conflicting_first_ack_and_wrong_event_digest_cannot_change_receipts(committed):
    p = committed
    event = p.events[0]
    first = ack(p, event, pb.MessagingEventAcknowledgement.STORED)
    await accept(p, first)
    before = await receipts(p, event)
    for changed in (
        ack(
            p,
            event,
            pb.MessagingEventAcknowledgement.TECHNICALLY_HELD,
            identity=first.envelope.message_id,
        ),
        ack(p, event, pb.MessagingEventAcknowledgement.STORED, digest="f" * 64),
    ):
        with pytest.raises(MessageConflict):
            await accept(p, changed)
        assert await receipts(p, event) == before


async def test_concurrent_duplicate_ack_has_one_original_business_receipt(committed):
    p = committed
    event = next(e for e in p.events if e.envelope().kind == pb.COMMAND_RECEIPT)
    first = ack(p, event, pb.MessagingEventAcknowledgement.STORED)
    await asyncio.gather(accept(p, first), accept(p, first))
    assert len(await receipts(p, event)) == 1
