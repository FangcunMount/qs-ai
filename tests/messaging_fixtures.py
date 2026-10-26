import os

import pytest
import sqlalchemy as sa
from jwcrypto import jwk
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.transport import Received
from reliable_messaging.wire import decode

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import (
    MessagingStore,
    metadata,
)
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox, sessions
from qs_ai.infrastructure.workflow_transport.messaging import (
    prepare,
)
from qs_ai.infrastructure.workflow_transport.mq_receiver import CommandReceiver
from qs_ai.infrastructure.workflow_transport.payloads import PayloadResolver

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
        await conn.run_sync(
            lambda sync: sessions.metadata.create_all(sync, tables=[sessions, result_outbox])
        )
        await conn.run_sync(metadata.create_all)
    try:
        yield database, Transactions(database), MessagingStore(), effects
    finally:
        async with database.engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(
                lambda sync: sessions.metadata.drop_all(sync, tables=[result_outbox, sessions])
            )
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
