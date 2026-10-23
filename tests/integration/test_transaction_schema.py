"""Host Alembic install and original-root SDK compatibility on owned databases."""

import hashlib
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from reliable_messaging.message import Message
from reliable_messaging.transaction import (
    Binding,
    BindingError,
    Decision,
    FrozenBranch,
    FrozenPlan,
    Request,
    StoreChecker,
    encode_reference,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema, require_schema

pytestmark = pytest.mark.integration


def plan(identity):
    message = Message(
        "schema-test",
        identity,
        "logical",
        "fact",
        "v1",
        "org:7",
        "application/octet-stream",
        "2026-10-19T00:00:00Z",
        b"\0\xff\x03",
    )
    return FrozenPlan(
        "ai.mysql",
        "org:7",
        "op-" + identity,
        identity,
        hashlib.sha256(identity.encode()).digest(),
        (FrozenBranch("fact", "physical", "m3-1", message, b"\0\xff\x04"),),
    )


@pytest.fixture
async def installed():
    url = os.environ.get("QS_AI_SCHEMA_TEST_SERVER")
    if not url:
        pytest.fail("Required explicitly disposable QS_AI_SCHEMA_TEST_SERVER missing")
    server = sa.create_engine(url)
    name = "ai_m3_tx_test_" + uuid4().hex
    engine = None
    owned = None
    created = False
    try:
        with server.connect() as conn:
            conn.execute(sa.text(f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4"))
            created = True
        owned = sa.create_engine(server.url.set(database=name))
        with owned.connect() as conn:
            config = Config("alembic.ini")
            config.attributes["connection"] = conn
            command.upgrade(config, "0040_module_table_names")
            conn.execute(
                sa.text("""INSERT INTO messaging_outbox
                (producer,destination,message_id,body_sha256,body,wire,wire_sha256,
                topic,kind,organization_id,aggregate_key,aggregate_sequence,ordered,
                requires_receipt,stage,attempts,available_at,created_at,error_code)
                VALUES('qs-ai','qs-server','old',REPEAT('a',64),0x00ff03,0x00ff04,
                REPEAT('b',64),'old.topic',1,7,'old',1,0,1,'held',7,
                UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),'original')""")
            )
            conn.commit()
            command.upgrade(config, "0041_transaction_messaging")
            conn.commit()
            # An explicit forward install adds business ACK history; the frozen
            # 0041 migration and old records are never rewritten.
            command.upgrade(config, "0042_transaction_event_receipts")
            command.upgrade(config, "0043_native_transaction_limit")
            conn.commit()
            # Migration and metadata must agree; this also covers native binary types/indexes.
            command.check(config)
            require_runtime_schema(conn)
            with pytest.raises(ValueError, match="table set"):
                require_schema(conn)  # Frozen 0040 conversion validator remains strict.
            conn.execute(sa.text("ALTER TABLE rm_tx_execution DROP INDEX rm_tx_expiry"))
            with pytest.raises(ValueError, match="Transaction index drift"):
                require_runtime_schema(conn)
            conn.execute(
                sa.text("CREATE INDEX rm_tx_expiry ON rm_tx_execution(owner,state,lease_until)")
            )
            require_runtime_schema(conn)
            row = conn.execute(
                sa.text("""SELECT HEX(body),HEX(wire),stage,attempts,error_code
                FROM messaging_outbox WHERE message_id='old'""")
            ).one()
            assert row == ("00FF03", "00FF04", "held", 7, "original")
            assert conn.scalar(sa.text("SELECT COUNT(*) FROM rm_tx_binding")) == 0
            conn.execute(sa.text("CREATE TABLE m3_install_fact(id VARBINARY(128) PRIMARY KEY)"))
            conn.commit()
        engine = create_async_engine(server.url.set(drivername="mysql+asyncmy", database=name))
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        yield sessions
    finally:
        if engine is not None:
            await engine.dispose()
        if owned is not None:
            owned.dispose()
        if created:
            with server.connect() as conn:
                conn.execute(sa.text(f"DROP DATABASE `{name}`"))
        server.dispose()


async def test_installed_schema_requires_binding_and_preserves_original_root(installed):
    sessions = installed
    store = MySQLTransactionStore(sessions, Binding("ai.mysql", "owned-primary", "m3-1"))
    with pytest.raises(BindingError):
        await store.validate()
    async with sessions() as db, db.begin():
        await db.execute(
            sa.text("INSERT INTO rm_tx_binding VALUES('ai.mysql','owned-primary','m3-1',1)")
        )
    await store.validate()
    wrong = MySQLTransactionStore(sessions, Binding("ai.mysql", "WRONG", "m3-1"))
    with pytest.raises(BindingError):
        await wrong.validate()
    for committed in (False, True):
        identity = f"root-{committed}"
        p = plan(identity)
        request = Request(p.owner, p.scope, p.idempotency_key, p.input_hash)
        reservation = await store.reserve(request, p)
        assert reservation.acquired
        execution = reservation.execution
        checker = StoreChecker(store)
        async with sessions() as db:
            await db.begin()
            guard = await store.bind(db, execution)
            await db.execute(
                sa.text("INSERT INTO m3_install_fact VALUES(:id)"), {"id": identity.encode()}
            )
            await guard.commit_witness()
            assert await checker.check(execution.references[0]) == Decision.UNKNOWN
            if committed:
                await db.commit()
            else:
                await db.rollback()
        assert await checker.check(execution.references[0]) == (
            Decision.COMMIT if committed else Decision.UNKNOWN
        )
        async with sessions() as db:
            count = await db.scalar(
                sa.text("SELECT COUNT(*) FROM m3_install_fact WHERE id=:id"),
                {"id": identity.encode()},
            )
            assert count == int(committed)
        if not committed:
            assert await store.abort(execution)
            assert await checker.check(execution.references[0]) == Decision.ROLLBACK
        duplicate = await store.reserve(request, p)
        assert not duplicate.acquired and not duplicate.execution.owned


def test_transaction_downgrade_retains_all_evidence():
    spec = importlib.util.spec_from_file_location(
        "m3_migration", Path("migrations/versions/0041_transaction_messaging.py")
    )
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with pytest.raises(RuntimeError, match="Preserve transaction evidence"):
        migration.downgrade()


async def test_transaction_archive_shares_original_root_and_never_exposes_prepared(installed):
    from jwcrypto import jwk

    from qs_ai.contracts.workflow import messaging_pb2 as pb
    from qs_ai.contracts.workflow import workflow_pb2 as workflow
    from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
    from qs_ai.infrastructure.persistence.mysql.transaction_schema import archive
    from qs_ai.infrastructure.workflow_transport.messaging import MessagingContractError, prepare
    from qs_ai.infrastructure.workflow_transport.transaction_archive import freeze_branch

    sessions = installed
    binding = Binding("ai.mysql", "owned-primary", "m3-1")
    store = MySQLTransactionStore(sessions, binding)
    archive_store = TransactionArchive(sessions, binding)
    async with sessions() as db, db.begin():
        await db.execute(
            sa.text("INSERT INTO rm_tx_binding VALUES('ai.mysql','owned-primary','m3-1',1)")
        )
    sign = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    recipient = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")
    for committed in (False, True):
        identity = str(uuid4())
        org = str(2**64 - 1)
        original = prepare(
            pb.INTERPRETATION_STATE,
            identity,
            identity,
            pb.MessagingBody(
                interpretation_state=workflow.StateEvent(
                    event_id=identity,
                    request_id=identity,
                    actor=workflow.Actor(org_id=org, subject_id="42"),
                    status="completed",
                    version=1,
                )
            ),
            organization_id=org,
            signing_key=sign,
            recipient_key=recipient,
            original_occurred_at="2026-10-19T12:00:00.123456789+08:00",
        )
        branch = freeze_branch(
            original, org, 2**64 - 1, True, True, "state", "m3.archive.test", "m3-1"
        )
        p = FrozenPlan(
            "ai.mysql",
            f"org:{org}",
            identity,
            identity,
            hashlib.sha256(identity.encode()).digest(),
            (branch,),
        )
        request = Request(p.owner, p.scope, p.idempotency_key, p.input_hash)
        reservation = await store.reserve(request, p)
        assert reservation.acquired
        async with sessions() as db:
            await db.begin()
            guard = await archive_store.bind(db, reservation.execution)
            await guard.put("state")
            await guard.put("state")
            assert await archive_store.read_committed(identity) is None
            await guard.commit_witness()
            with pytest.raises(BindingError):
                await guard.put("state")
            if committed:
                await db.commit()
            else:
                await db.rollback()
        value = await archive_store.read_committed(identity)
        if not committed:
            assert value is None
            continue
        assert value is not None
        assert value.body == original.body and value.wire == original.wire
        assert value.organization_id == 2**64 - 1
        reference = pb.MessagePayloadReference(
            producer="qs-ai",
            destination="qs-server",
            message_id=identity,
            organization_id=org,
            body_sha256=original.envelope.body_sha256,
            body_length=len(original.body),
        )
        assert await archive_store.payload(reference, "qs-server") == original.body
        with pytest.raises(MessagingContractError):
            await archive_store.payload(reference, "wrong")
        duplicate = await store.reserve(request, p)
        assert not duplicate.acquired
        async with sessions() as db, db.begin():
            with pytest.raises(BindingError):
                await archive_store.bind(db, duplicate.execution)
        async with sessions() as db, db.begin():
            await db.execute(
                archive.update()
                .where(archive.c.message_id == identity.encode())
                .values(first_wire=b"\0")
            )
        with pytest.raises(ValueError):
            await archive_store.read_committed(identity)
        async with sessions() as db, db.begin():
            await db.execute(
                archive.update()
                .where(archive.c.message_id == identity.encode())
                .values(first_wire=original.wire)
            )
            await db.execute(
                sa.text("UPDATE rm_tx_witness SET state='prepared' WHERE reference=:ref"),
                {"ref": encode_reference(reservation.execution.references[0])},
            )
        with pytest.raises(MessagingContractError, match="committed witness"):
            await archive_store.read_committed(identity)
