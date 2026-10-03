import hashlib
import os
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from jwcrypto import jwk
from reliable_messaging.delivery import Confirmation, DeliveryResult, Outcome
from reliable_messaging.durable import AWAITING_RECEIPT, CONFIRMED, Identity, MessageConflict
from reliable_messaging.nsq import Received
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.sqlalchemy import TransactionBindingError
from reliable_messaging.wire import decode, encode_failure

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import (
    MessagingStore,
    inbox,
    metadata,
    outbox,
    quarantine,
)
from qs_ai.infrastructure.workflow_transport.messaging import (
    ACKS,
    CHANNELS,
    COMMANDS,
    parse_body,
    prepare,
)
from qs_ai.infrastructure.workflow_transport.mq_receiver import AdmissionDecision, CommandReceiver
from qs_ai.infrastructure.workflow_transport.mq_relay import MQRelay
from qs_ai.infrastructure.workflow_transport.payloads import PayloadResolver

pytestmark = pytest.mark.integration
COMMAND_ID = "b1941896-e7df-4b9d-9417-b80bd05276b9"
EVENT_ID = "279e6c52-5519-49ec-80a0-4b63122de9cb"
ACK_ID = "0f8180d0-b53d-47d6-9f25-d9c00139f551"


@pytest.fixture
def keys():
    return {
        name: jwk.JWK.generate(kty="EC", crv="P-256", kid=name)
        for name in ("qs.sign", "ai.sign", "qs.encrypt", "ai.encrypt")
    }


@pytest.fixture
async def storage():
    url = os.environ.get("QS_MQ_MYSQL_URL")
    if not url:
        pytest.fail("Required disposable QS_MQ_MYSQL_URL is missing")
    database = Database(url)
    assert database.engine is not None
    effects = sa.Table(
        "mq_test_effects",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("count", sa.Integer),
    )
    async with database.engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield database, Transactions(database), MessagingStore(), effects
    finally:
        async with database.engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        metadata.remove(effects)
        await database.close()


def command(keys, *, goal="原意图"):
    return prepare(
        pb.START,
        COMMAND_ID,
        COMMAND_ID,
        pb.MessagingBody(
            start=workflow.StartCommand(
                request_id=COMMAND_ID,
                actor=workflow.Actor(org_id="1", subject_id="original"),
                goal=goal,
            )
        ),
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )


def received(message):
    return Received(decode(message.wire), message.wire, "physical", "127.0.0.1:4150", 1, 0)


def receiver(transactions, store, keys, owner):
    return CommandReceiver(
        transactions,
        store,
        owner,
        PayloadResolver({"qs-server": object()}),
        decrypt_keys={"ai.encrypt": keys["ai.encrypt"]},
        trusted_signers={"qs.sign": TrustedSigner("qs-server", keys["qs.sign"])},
        signing_key=keys["ai.sign"],
        qs_recipient_key=keys["qs.encrypt"],
    )


async def counts(transactions, *tables):
    async with transactions.open() as db:
        return [await db.scalar(sa.select(sa.func.count()).select_from(t)) for t in tables]


def failure(message, topic):
    return encode_failure(
        decode(message.wire),
        topic=topic,
        channel=CHANNELS[topic],
        transport_id="0" * 16,
        attempts=65535,
        timestamp=0,
        cause="handler_failed",
    )


async def test_physical_failure_never_admits_and_cannot_forge_logical_budget(storage, keys):
    _, transactions, store, effects = storage
    calls = []

    class Owner:
        async def admit(self, db, envelope, body):
            calls.append(1)
            raise AssertionError("physical failure cannot enter admission")

    transport = receiver(transactions, store, keys, Owner())
    original = command(keys)
    notice = failure(original, COMMANDS)
    await transport.failed_command(notice)
    assert not calls
    assert await counts(transactions, effects, inbox, outbox, quarantine) == [0, 0, 0, 1]
    # Only authenticated, locally persisted failures exhaust the logical budget.
    async with transactions.open() as db:
        await db.begin()
        for _ in range(8):
            await store.technical_failure(db, original.envelope, original.body, original.wire)
        await db.commit()
    await transport.failed_command(notice)
    await transport.receive_command(received(original))
    assert not calls
    assert await counts(transactions, effects, inbox, outbox, quarantine) == [0, 1, 1, 2]
    async with transactions.open() as db:
        row = (await db.execute(sa.select(inbox))).mappings().one()
        assert row["decision"] == "held"
    # Wrapper identity is untrusted even when it contains valid original ciphertext.
    changed = notice.replace(COMMAND_ID.encode(), EVENT_ID.encode())
    await transport.failed_command(changed)
    assert not calls
    assert await counts(transactions, effects, inbox, outbox, quarantine) == [0, 1, 1, 3]


async def test_failed_ack_remains_unknown_and_storage_failure_propagates(storage, keys):
    _, transactions, store, _ = storage

    class Owner:
        async def admit(self, *args):
            raise AssertionError("ACK cannot enter admission")

    transport = receiver(transactions, store, keys, Owner())
    event = prepare(
        pb.INTERPRETATION_STATE,
        EVENT_ID,
        COMMAND_ID,
        pb.MessagingBody(interpretation_state=workflow.StateEvent(event_id=EVENT_ID)),
        organization_id="1",
        signing_key=keys["ai.sign"],
        recipient_key=keys["qs.encrypt"],
    )
    async with transactions.open() as db:
        await db.begin()
        await store.stage(db, event, organization_id="1", sequence=1)
        await db.commit()
    ack = prepare(
        pb.EVENT_ACKNOWLEDGEMENT,
        ACK_ID,
        COMMAND_ID,
        pb.MessagingBody(
            event_acknowledgement=pb.MessagingEventAcknowledgement(
                event_id=EVENT_ID,
                event_body_sha256=event.envelope.body_sha256,
                event_kind=pb.INTERPRETATION_STATE,
                outcome=pb.MessagingEventAcknowledgement.STORED,
            )
        ),
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )
    notice = failure(ack, ACKS)
    await transport.failed_ack(notice)
    async with transactions.open() as db:
        row = (await db.execute(sa.select(outbox))).mappings().one()
        assert row["stage"] == "staged"  # failure notification did not confirm
    original_quarantine = store.quarantine_wire

    async def unavailable(*args):
        raise OSError("storage unavailable")

    store.quarantine_wire = unavailable
    with pytest.raises(OSError):
        await transport.failed_ack(notice)
    store.quarantine_wire = original_quarantine
    await transport.receive_ack(received(ack))
    async with transactions.open() as db:
        assert (await db.execute(sa.select(outbox.c.stage))).scalar_one() == CONFIRMED


async def test_closed_local_savepoint_preserves_root_and_refusal_receipt(storage, keys):
    _, transactions, store, effects = storage

    class Owner:
        async def admit(self, db, envelope, body):
            original = db.get_transaction()
            nested = await db.begin_nested()
            await db.execute(effects.insert().values(id=envelope.message_id, count=1))
            await nested.rollback()
            assert db.get_transaction() is original and db.get_nested_transaction() is None
            return AdmissionDecision("1", 1, pb.REJECTED, "capacity_full", 8)

    await receiver(transactions, store, keys, Owner()).receive_command(received(command(keys)))
    assert await counts(transactions, effects, inbox, outbox) == [0, 1, 1]


async def test_admission_receipt_and_business_share_original_commit_and_duplicate_precedes_cas(
    storage, keys
):
    database, transactions, store, effects = storage
    calls = []

    class Owner:
        async def admit(self, db, envelope, body):
            calls.append(envelope.message_id)
            await db.execute(effects.insert().values(id=envelope.message_id, count=1))
            return AdmissionDecision(
                "1",
                1,
                pb.ACCEPTED,
                workflow_receipt=workflow.Receipt(session_id=EVENT_ID, run_id=ACK_ID, version=1),
            )

    transport = receiver(transactions, store, keys, Owner())
    original = command(keys)
    await transport.receive_command(received(original))
    async with transactions.open() as db:
        first = (await db.execute(sa.select(outbox))).mappings().one()
        decision = (await db.execute(sa.select(inbox))).mappings().one()
    await transport.receive_command(received(original))
    assert calls == [COMMAND_ID]
    assert await counts(transactions, effects, inbox, outbox) == [1, 1, 1]
    async with transactions.open() as db:
        current = (await db.execute(sa.select(outbox))).mappings().one()
        assert current["wire"] == first["wire"] and current["message_id"] == first["message_id"]
        assert decision["receipt_id"] == first["message_id"] and decision["decision"] == "accepted"
    # A separately signed conflicting body cannot invoke the owner or overwrite the first receipt.
    await transport.receive_command(received(command(keys, goal="changed")))
    assert calls == [COMMAND_ID]
    assert await counts(transactions, effects, inbox, outbox, quarantine) == [1, 1, 1, 1]
    async with database.engine.connect() as conn:
        assert (await conn.execute(sa.text("SELECT 1"))).scalar() == 1


async def test_technical_failure_rolls_back_business_inbox_and_receipt(storage, keys):
    _, transactions, store, effects = storage

    class Owner:
        async def admit(self, db, envelope, body):
            await db.execute(effects.insert().values(id=envelope.message_id, count=1))
            raise OSError("temporary storage dependency")

    with pytest.raises(OSError):
        await receiver(transactions, store, keys, Owner()).receive_command(received(command(keys)))
    assert await counts(transactions, effects, inbox, outbox) == [0, 0, 0]


async def test_deterministic_rejection_persists_and_invalid_wire_never_calls_owner(storage, keys):
    _, transactions, store, effects = storage
    calls = []

    class Owner:
        async def admit(self, db, envelope, body):
            calls.append(1)
            return AdmissionDecision("1", 1, pb.REJECTED, "capacity_full", 8)

    transport = receiver(transactions, store, keys, Owner())
    original = received(command(keys))
    await transport.receive_command(original)
    await transport.receive_command(original)
    invalid = Received(original.envelope, b"PRIVATE bad wire", "physical", "node", 1, 0)
    await transport.receive_command(invalid)
    assert calls == [1]
    assert await counts(transactions, effects, inbox, outbox, quarantine) == [0, 1, 1, 1]
    async with transactions.open() as db:
        row = (await db.execute(sa.select(inbox))).mappings().one()
        assert row["decision"] == "rejected"


async def test_oversized_body_authorization_and_exact_acknowledgement(storage, keys):
    _, transactions, store, _ = storage
    event = prepare(
        pb.INTERPRETATION_STATE,
        EVENT_ID,
        COMMAND_ID,
        pb.MessagingBody(
            interpretation_state=workflow.StateEvent(
                event_id=EVENT_ID,
                request_id=COMMAND_ID,
                artifact_json="🙂" * 32768,
            )
        ),
        organization_id="1",
        signing_key=keys["ai.sign"],
        recipient_key=keys["qs.encrypt"],
    )
    async with transactions.open() as db:
        with pytest.raises(TransactionBindingError):
            await store.stage(db, event, organization_id="1", sequence=1)
        assert not db.in_transaction()
        await db.begin()
        await store.stage(db, event, organization_id="1", sequence=1)
        await store.outbox.published(
            db, Identity("qs-ai", "qs-server", EVENT_ID), event.envelope.body_sha256
        )
        await db.commit()
    reference = event.envelope.payload_reference
    async with transactions.open() as db:
        await db.begin()
        assert await store.payload(db, reference, "qs-server") == event.body
        with pytest.raises(MessageConflict):
            await store.payload(db, reference, "untrusted")
        changed = pb.MessagePayloadReference()
        changed.CopyFrom(reference)
        changed.organization_id = "2"
        with pytest.raises(MessageConflict):
            await store.payload(db, changed, "qs-server")
        assert await db.scalar(sa.select(outbox.c.stage)) == AWAITING_RECEIPT
    ack = prepare(
        pb.EVENT_ACKNOWLEDGEMENT,
        ACK_ID,
        event.envelope.aggregate_key,
        pb.MessagingBody(
            event_acknowledgement=pb.MessagingEventAcknowledgement(
                event_id=EVENT_ID,
                event_body_sha256=event.envelope.body_sha256,
                event_kind=pb.INTERPRETATION_STATE,
                outcome=pb.MessagingEventAcknowledgement.STORED,
            )
        ),
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )
    async with transactions.open() as db:
        await db.begin()
        await store.confirm_event(db, ack.envelope, ack.body)
        await db.commit()
    async with transactions.open() as db:
        assert await db.scalar(sa.select(outbox.c.stage)) == CONFIRMED
        original = (await db.execute(sa.select(outbox))).mappings().one()
        assert original["wire"] == event.wire
        assert original["body_sha256"] == hashlib.sha256(event.body).hexdigest()
        parse_body(event.envelope, original["body"])


async def test_logical_failure_budget_survives_fresh_physical_delivery_and_never_admits_after_hold(
    storage, keys
):
    _, transactions, store, effects = storage
    calls = []

    class Owner:
        async def admit(self, db, envelope, body):
            calls.append(envelope.message_id)
            await db.execute(effects.insert().values(id=envelope.message_id, count=1))
            raise OSError("temporary persistence failure")

    transport = receiver(transactions, store, keys, Owner())
    original = command(keys)
    for _ in range(7):
        # A new physical PUB starts NSQ attempts at 1; the persistent logical budget remains.
        with pytest.raises(OSError):
            await transport.receive_command(received(original))
    await transport.receive_command(received(original))
    await transport.receive_command(received(original))
    assert calls == [COMMAND_ID] * 8
    assert await counts(transactions, effects, inbox, outbox, quarantine) == [0, 1, 1, 1]
    async with transactions.open() as db:
        row = (await db.execute(sa.select(inbox))).mappings().one()
        failure = (await db.execute(sa.select(quarantine))).mappings().one()
        assert row["decision"] == "held" and failure["attempts"] == 8
        assert failure["logical_message_id"] == COMMAND_ID
        first = (await db.execute(sa.select(outbox))).mappings().one()
        receipt = pb.MessagingBody.FromString(first["body"]).command_receipt
        assert receipt.decision == pb.HELD and receipt.code == "technical_budget_exhausted"
        assert receipt.command_body_sha256 == original.envelope.body_sha256


async def test_single_host_relay_keeps_business_receipt_wait_and_bounded_original_publish(
    storage, keys
):
    database, transactions, store, _ = storage
    event = prepare(
        pb.INTERPRETATION_STATE,
        EVENT_ID,
        COMMAND_ID,
        pb.MessagingBody(interpretation_state=workflow.StateEvent(event_id=EVENT_ID)),
        organization_id="1",
        signing_key=keys["ai.sign"],
        recipient_key=keys["qs.encrypt"],
    )
    seen = []

    async def publish(topic, wire):
        seen.append((topic, wire))
        return DeliveryResult(Outcome.CONFIRMED, Confirmation.BROKER)

    publisher = SimpleNamespace(publish=publish)
    relay = MQRelay(transactions, store, {"node": publisher}, address="node")
    async with transactions.open() as db:
        await db.begin()
        await store.stage(db, event, organization_id="1", sequence=1)
        await db.commit()
    await relay.step()
    async with transactions.open() as db:
        assert await db.scalar(sa.select(outbox.c.stage)) == AWAITING_RECEIPT
    await database.engine.dispose()  # discard notifications/connections, never persistent rows
    async with transactions.open() as db:
        await db.begin()
        await db.execute(outbox.update().values(available_at=sa.func.utc_timestamp(6)))
        await db.commit()
    await relay.step()
    assert seen == [(event.topic, event.wire)] * 2
    async with transactions.open() as db:
        await db.begin()
        await db.execute(outbox.update().values(attempts=8, available_at=sa.func.utc_timestamp(6)))
        await db.commit()
    await relay.step()
    await relay.step()
    assert len(seen) == 2
    async with transactions.open() as db:
        assert await db.scalar(sa.select(outbox.c.stage)) == "held"
