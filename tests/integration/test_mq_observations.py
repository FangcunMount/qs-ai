"""Durable technical facts never authorize admission, settlement or another call."""

import grpc
import pytest
import sqlalchemy as sa
from reliable_messaging.durable import MessageConflict

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.messaging import inbox, outbox
from qs_ai.infrastructure.persistence.mysql.messaging_observations import (
    collect_observations,
    observations,
    record_observation,
    require_recording_schema,
)
from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError, prepare
from qs_ai.infrastructure.workflow_transport.mq_receiver import AdmissionDecision
from qs_ai.transport.grpc.message_payloads import MessagePayloads
from tests.integration.test_mq_storage import (
    ACK_ID,
    COMMAND_ID,
    EVENT_ID,
    command,
    counts,
    received,
    receiver,
)
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_mq_storage import storage as storage
from tests.test_grpc_commands import Aborted, Context

pytestmark = pytest.mark.integration


async def counter(transactions, kind):
    async with transactions.open() as db:
        return int(
            await db.scalar(
                sa.select(observations.c.recorded_count).where(observations.c.kind == kind)
            )
            or 0
        )


async def test_mq_duplicate_observations_share_original_commit_and_conflict_is_not_duplicate(
    storage, keys
):
    _, tx, store, effects = storage
    calls = []

    class Owner:
        async def admit(self, db, envelope, body):
            calls.append(1)
            return AdmissionDecision("1", 1, pb.REJECTED, "capacity_full", 8)

    transport = receiver(tx, store, keys, Owner())
    original = command(keys)
    await transport.receive_command(received(original))
    async with tx.open() as db:
        first = (await db.execute(sa.select(outbox))).mappings().one()
    async with tx.open() as db:
        await db.begin()
        await store.reserve_command(db, original.envelope, original.body, original.wire)
        assert await counter(tx, "duplicate_command") == 0
        await db.rollback()
    assert await counter(tx, "duplicate_command") == 0
    await transport.receive_command(received(original))
    assert await counter(tx, "duplicate_command") == 1
    await transport.receive_command(received(command(keys, goal="different original intent")))
    assert await counter(tx, "duplicate_command") == 1
    assert calls == [1]
    assert await counts(tx, effects, inbox, outbox) == [0, 1, 1]
    async with tx.open() as db:
        last = (await db.execute(sa.select(outbox))).mappings().one()
        assert last["wire"] == first["wire"] and last["created_at"] == first["created_at"]


async def test_mq_duplicate_ack_records_only_after_exact_confirmation_and_rolls_back(storage, keys):
    _, tx, store, _ = storage
    event = prepare(
        pb.INTERPRETATION_STATE,
        EVENT_ID,
        COMMAND_ID,
        pb.MessagingBody(interpretation_state=workflow.StateEvent(event_id=EVENT_ID)),
        organization_id="1",
        signing_key=keys["ai.sign"],
        recipient_key=keys["qs.encrypt"],
    )
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
    async with tx.open() as db:
        await db.begin()
        await store.stage(db, event, organization_id="1", sequence=1)
        await store.confirm_event(db, ack.envelope, ack.body)
        await db.commit()
    assert await counter(tx, "duplicate_ack") == 0
    async with tx.open() as db:
        first = (await db.execute(sa.select(outbox))).mappings().one()
    async with tx.open() as db:
        await db.begin()
        await store.confirm_event(db, ack.envelope, ack.body)
        assert await counter(tx, "duplicate_ack") == 0
        await db.rollback()
    assert await counter(tx, "duplicate_ack") == 0
    async with tx.open() as db:
        await db.begin()
        await store.confirm_event(db, ack.envelope, ack.body)
        await db.commit()
    assert await counter(tx, "duplicate_ack") == 1
    body = pb.MessagingBody.FromString(ack.body)
    body.event_acknowledgement.event_body_sha256 = "0" * 64
    wrong = prepare(
        pb.EVENT_ACKNOWLEDGEMENT,
        ACK_ID,
        COMMAND_ID,
        body,
        organization_id="1",
        signing_key=keys["qs.sign"],
        recipient_key=keys["ai.encrypt"],
    )
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(MessageConflict):
            await store.confirm_event(db, wrong.envelope, wrong.body)
        await db.rollback()
    assert await counter(tx, "duplicate_ack") == 1
    async with tx.open() as db:
        last = (await db.execute(sa.select(outbox))).mappings().one()
        assert last["confirmed_at"] == first["confirmed_at"] and last["wire"] == event.wire


class WorkloadDenied(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.PERMISSION_DENIED


@pytest.mark.parametrize(
    "error,kind",
    [
        (TimeoutError("private endpoint"), "payload_fetch_unavailable"),
        (MessagingContractError("private reference"), "payload_fetch_reference_mismatch"),
        (WorkloadDenied(), "payload_fetch_workload_denied"),
    ],
)
async def test_mq_payload_fetch_audit_retains_original_error_and_never_admits(
    storage, keys, error, kind
):
    _, tx, store, _ = storage
    original = command(keys, goal="🙂" * 17000)
    assert original.envelope.HasField("payload_reference")
    transport = receiver(tx, store, keys, object())

    class Resolver:
        async def body(self, envelope):
            raise error

    transport.resolver = Resolver()
    with pytest.raises(type(error)) as failure:
        await transport._body(original.envelope)
    assert failure.value is error
    assert await counter(tx, kind) == 1
    assert await counts(tx, inbox, outbox) == [0, 0]


@pytest.mark.parametrize(
    "scenario,code,kind",
    [
        ("workload", grpc.StatusCode.PERMISSION_DENIED, "payload_serve_workload_denied"),
        ("reference", grpc.StatusCode.NOT_FOUND, "payload_serve_reference_mismatch"),
        ("storage", grpc.StatusCode.UNAVAILABLE, "payload_serve_storage_unavailable"),
    ],
)
async def test_mq_payload_serve_audit_preserves_read_rollback_and_fixed_rpc_error(
    storage, scenario, code, kind
):
    _, tx, store, _ = storage
    context = Context()
    if scenario == "workload":
        context.auth_context = lambda: {}
    original_error = OSError("private DSN and body") if scenario == "storage" else MessageConflict()
    reads = []

    async def payload(db, request, workload):
        reads.append(db)
        assert db.in_transaction()
        raise original_error

    store.payload = payload
    request = pb.MessagePayloadReference(
        producer="qs-ai",
        destination="qs-server",
        message_id=EVENT_ID,
        organization_id="1",
        body_sha256="a" * 64,
        body_length=100,
    )
    with pytest.raises(Aborted) as failure:
        from qs_ai.infrastructure.workflow_transport.payload_access import MySQLPayloadAccess

        await MessagePayloads(MySQLPayloadAccess(tx, store)).Get(request, context)
    assert failure.value.args[0] == code and "private" not in str(failure.value)
    assert len(reads) == (0 if scenario == "workload" else 1)
    assert all(not db.in_transaction() for db in reads)
    assert await counter(tx, kind) == 1
    assert await counts(tx, inbox, outbox) == [0, 0]


async def test_mq_audit_storage_failure_never_masks_payload_failure_or_fabricates_counts(
    storage, keys, caplog
):
    database, tx, store, _ = storage
    original = command(keys, goal="🙂" * 17000)
    error = TimeoutError("private URL")

    class Resolver:
        async def body(self, envelope):
            raise error

    transport = receiver(tx, store, keys, object())
    transport.resolver = Resolver()
    async with database.engine.begin() as conn:
        await conn.run_sync(observations.drop)
    try:
        async with tx.open() as db:
            with pytest.raises(
                RuntimeError, match="^MQ required technical observation schema unavailable$"
            ):
                await require_recording_schema(db)
        with pytest.raises(TimeoutError) as failure:
            await transport._body(original.envelope)
        assert failure.value is error
        assert "mq_payload_observation_unavailable" in caplog.text
        assert "private" not in caplog.text
        assert await counts(tx, inbox, outbox) == [0, 0]
    finally:
        async with database.engine.begin() as conn:
            await conn.run_sync(observations.create)


async def test_mq_partial_technical_ledger_is_unavailable_and_kind_is_fixed(storage):
    _, tx, _, _ = storage
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(ValueError):
            await record_observation(db, "untrusted body label")
        await record_observation(db, "duplicate_ack")
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(
            RuntimeError, match="^MQ required technical observation schema unavailable$"
        ):
            await require_recording_schema(db)
        assert await collect_observations(db) == {
            "mq_duplicate_observations_available": 0,
            "mq_payload_error_observations_available": 0,
        }
        row = (await db.execute(sa.select(observations))).mappings().one()
        assert set(row) == {"kind", "recorded_count", "recording_since", "last_observed_at"}
        assert row["kind"] == "duplicate_ack" and row["recorded_count"] == 1
