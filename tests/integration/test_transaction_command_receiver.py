"""Actual MySQL receiver planning/roots; database-only runtime, no halves/Broker claim."""

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest
from reliable_messaging.transaction import Result, State
from sqlalchemy import delete

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
from tests.integration.test_mq_admission import admission, start_message
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_mq_storage import received, receiver
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import incoming, planner, reserve, rows
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def owned_failure_cleanup(host):
    try:
        yield
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
    host,
):
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


async def test_prepared_winner_cannot_be_reacquired_or_reexecuted_by_redelivery(host):
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
async def test_committed_legacy_inbox_is_not_resealed_or_adopted_as_sdk_witness(host):
    handler, boundary, runtime = transport(host)
    message = start_message(host.kit, host.keys)
    try:
        await boundary.receive_command(received(message))
        stored = (await rows(host, inbox, inbox.c.message_id == message.envelope.message_id))[0]
        assert stored["decision"] == "accepted"
        await handler.receive_command(delivered(message))
        assert runtime.execute_count == runtime.reserve_count == 0
        assert not host.executions
        # The receiver only reads compatibility evidence and leaves the original
        # receipt/Relay responsibility unchanged for coordinated drain.
        assert len(await rows(host, outbox, outbox.c.message_id == stored["receipt_id"])) == 1
    finally:
        async with host.tx.open() as db:
            if "stored" in locals():
                await db.execute(delete(outbox).where(outbox.c.message_id == stored["receipt_id"]))
            await db.execute(delete(inbox).where(inbox.c.message_id == message.envelope.message_id))
            await db.commit()
