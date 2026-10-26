"""Actual MySQL receiver planning/roots; database-only runtime, no halves/Broker claim."""

import hashlib
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Result, State
from sqlalchemy import delete, func, insert

from qs_ai.infrastructure.persistence.mysql.messaging import (
    MessagingStore,
    inbox,
    outbox,
    quarantine,
)
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS
from qs_ai.infrastructure.workflow_transport.transaction_command import read_admission_plan
from qs_ai.infrastructure.workflow_transport.transaction_receiver import (
    COMMAND_TOPIC,
    CommandTransactionPending,
    TransactionCommandReceiver,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import incoming, planner, reserve, rows
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.messaging_fixtures import keys as keys
from tests.messaging_fixtures import received, receiver
from tests.transaction_fixtures import admission, start_message

pytestmark = pytest.mark.integration


@pytest.fixture
async def receiver_host(host):
    # Do not make a database-owning cleanup autouse: that would create the
    # participant before published_configuration and tear its referenced
    # publication down before the participant's execution configuration.
    try:
        yield host
    finally:
        async with host.tx.open() as db:
            for message in host.messages:
                await db.execute(
                    delete(quarantine).where(
                        quarantine.c.logical_producer == "qs-server",
                        quarantine.c.logical_message_id == message.envelope().message_id,
                    )
                )
            await db.commit()


class DatabaseOnlyRuntime:
    def __init__(self, host):
        self.host, self.store = host, host.store
        self.reserve_count = self.execute_count = 0

    async def reserve(self, request, candidate):
        self.reserve_count += 1
        return await reserve(self.host, SimpleNamespace(request=request), candidate)

    async def execute(self, execution, local):
        self.execute_count += 1
        return Result(await local(execution), ())  # No transport or half-message simulation.


def transport(host):
    owner = admission(host.kit.source)
    boundary = receiver(host.tx, MessagingStore(), host.keys, owner)
    runtime = DatabaseOnlyRuntime(host)
    handler = TransactionCommandReceiver(boundary, owner, runtime, host.archives)
    return handler, boundary, runtime


def delivered(message):
    return replace(
        received(message), provider="rocketmq", physical_topic=COMMAND_TOPIC, logical_topic=COMMANDS
    )


@pytest.mark.usefixtures("published_configuration")
async def test_receiver_authenticates_and_reads_persisted_winner_on_duplicate_without_business_redo(
    receiver_host,
):
    host = receiver_host
    handler, _, runtime = transport(host)
    value = incoming(host)
    message = SimpleNamespace(wire=value.wire)
    header = value.envelope()
    await handler.receive_command(delivered(message))
    first = await host.store.lookup(value.request)
    assert first.state == State.COMMITTED
    frozen = read_admission_plan(value, first.execution.plan)
    for archive in frozen:
        current = await host.archives.read_committed(archive.envelope().message_id)
        assert (
            current is not None
            and hashlib.sha256(current.wire).digest() == hashlib.sha256(archive.wire).digest()
        )
    await handler.receive_command(delivered(message))
    assert runtime.execute_count == runtime.reserve_count == 1
    assert len(await rows(host, inbox, inbox.c.message_id == header.message_id)) == 1
    for archive in frozen:
        assert not await rows(host, outbox, outbox.c.message_id == archive.envelope().message_id)


async def test_prepared_winner_cannot_be_reacquired_or_reexecuted_by_redelivery(receiver_host):
    host = receiver_host
    value = incoming(host)
    owned = (await reserve(host, value, await planner(host).candidate(value))).execution
    handler, _, runtime = transport(host)
    message = SimpleNamespace(wire=value.wire)
    with pytest.raises(CommandTransactionPending):
        await handler.receive_command(delivered(message))
    assert runtime.execute_count == runtime.reserve_count == 0
    assert not await rows(host, inbox, inbox.c.message_id == value.envelope().message_id)
    assert (await host.store.lookup(value.request)).state == State.PREPARED
    assert await host.store.abort(owned)


@pytest.mark.usefixtures("published_configuration")
async def test_committed_legacy_inbox_is_not_resealed_or_adopted_as_sdk_witness(receiver_host):
    host = receiver_host
    handler, boundary, runtime = transport(host)
    message = start_message(host.kit, host.keys)
    try:
        async with host.tx.open() as db:
            await db.execute(
                insert(inbox).values(
                    producer=message.envelope.producer,
                    message_id=message.envelope.message_id,
                    destination=message.envelope.destination,
                    body_sha256=message.envelope.body_sha256,
                    body=message.body,
                    wire_sha256=hashlib.sha256(message.wire).hexdigest(),
                    kind=message.envelope.kind,
                    aggregate_key=message.envelope.aggregate_key,
                    reservation_token=str(uuid4()),
                    decision="accepted",
                    receipt_id=str(uuid4()),
                    received_at=func.utc_timestamp(6),
                )
            )
            await db.commit()
        stored = (await rows(host, inbox, inbox.c.message_id == message.envelope.message_id))[0]
        assert stored["decision"] == "accepted"
        await handler.receive_command(delivered(message))
        assert runtime.execute_count == runtime.reserve_count == 0
        assert not host.executions
        assert not await rows(host, outbox, outbox.c.message_id == stored["receipt_id"])
        assert await rows(
            host, quarantine, quarantine.c.wire_sha256 == hashlib.sha256(message.wire).hexdigest()
        )
    finally:
        async with host.tx.open() as db:
            if "stored" in locals():
                await db.execute(delete(outbox).where(outbox.c.message_id == stored["receipt_id"]))
            await db.execute(delete(inbox).where(inbox.c.message_id == message.envelope.message_id))
            await db.commit()


async def test_local_budget_hold_uses_own_empty_state_transcription_without_legacy_commit_hook(
    receiver_host,
):
    host = receiver_host
    from unittest.mock import AsyncMock

    value = incoming(host)
    async with host.tx.open() as db:
        await db.begin()
        for _ in range(8):
            await MessagingStore().technical_failure(db, value.envelope(), value.body, value.wire)
        await db.commit()
    legacy = SimpleNamespace(
        flush=AsyncMock(side_effect=AssertionError("legacy commit hook forbidden"))
    )
    handler, _, runtime = transport(host)
    host.tx.database.state_events = legacy
    try:
        await handler.receive_command(delivered(SimpleNamespace(wire=value.wire)))
        observed = await host.store.lookup(value.request)
        assert observed.state == State.COMMITTED and len(observed.execution.plan.branches) == 1
        assert runtime.execute_count == 1
        legacy.flush.assert_not_called()
        stored = (await rows(host, inbox, inbox.c.message_id == value.envelope().message_id))[0]
        assert stored["decision"] == "held"
    finally:
        host.tx.database.state_events = None
