"""Real startup read-only enforcement on the application's one disposable MySQL pool."""

import os
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, insert, select, text, update
from sqlalchemy.exc import DBAPIError

from qs_ai.bootstrap import messaging
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging_observations import require_recording_schema

pytestmark = pytest.mark.integration


async def test_mq_startup_schema_transaction_rejects_write_and_preserves_pool(monkeypatch):
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.fail("Required disposable startup MySQL DSN is missing")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1), pool_size=1, max_overflow=0)
    assert database.engine is not None
    transactions = Transactions(database)
    probe = Table(
        f"mq_readonly_probe_{uuid4().hex}",
        MetaData(),
        Column("value", Integer, primary_key=True),
        mysql_engine="InnoDB",
    )
    connection_ids = []

    class VerifiedStartup(RuntimeError):
        pass

    async def check(db):
        await require_recording_schema(db)
        connection_ids.append(await db.scalar(text("SELECT CONNECTION_ID()")))
        with pytest.raises(DBAPIError) as rejected:
            await db.execute(update(probe).values(value=2))
        assert rejected.value.orig.args[0] == 1792  # ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
        assert await db.scalar(select(probe.c.value)) == 1
        raise VerifiedStartup  # Stop before constructing any transport or starting consumers.

    monkeypatch.setattr(messaging, "require_recording_schema", check)
    monkeypatch.setattr(messaging, "read_key", lambda *args, **kwargs: object())
    monkeypatch.setattr(messaging, "TrustedSigner", MagicMock())
    for name in ("StateEventRecorder", "NSQPublisher", "NSQSubscriber", "mtls_channel"):
        monkeypatch.setattr(messaging, name, MagicMock())
    container = MagicMock()
    container.get = AsyncMock(return_value=database)
    settings = Settings(
        grpc={"access_address": "qs.invalid:9090"},
        messaging={
            "enabled": True,
            "nsqd": {"nsq.invalid:4150": "http://nsq.invalid:4151"},
            "signing_key_file": "/unused/sign.jwk",
            "decrypt_key_files": {"ai.encrypt": "/unused/decrypt.jwk"},
            "qs_signer_files": {"qs.sign": "/unused/qs-sign.jwk"},
            "qs_recipient_key_file": "/unused/qs-encrypt.jwk",
        },
    )
    try:
        async with database.engine.begin() as conn:
            await conn.run_sync(probe.metadata.create_all)
        async with transactions.open() as db:
            await db.execute(insert(probe).values(value=1))
            await db.commit()
        with pytest.raises(VerifiedStartup):
            await messaging.MessagingRuntime.create(container, settings, b"", b"", b"")
        for name in ("StateEventRecorder", "NSQPublisher", "NSQSubscriber", "mtls_channel"):
            getattr(messaging, name).assert_not_called()
        assert database.state_events is None
        async with transactions.open() as db:
            assert await db.scalar(text("SELECT CONNECTION_ID()")) == connection_ids[0]
            await db.execute(update(probe).values(value=3))
            await db.commit()
        async with transactions.open() as db:
            assert await db.scalar(select(probe.c.value)) == 3
    finally:
        async with database.engine.begin() as conn:
            await conn.run_sync(probe.metadata.drop_all)
        await database.close()
