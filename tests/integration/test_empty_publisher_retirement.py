"""Real MySQL only: exact-head empty-table retirement and protected native facts."""

import hashlib
import json
import os
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from reliable_messaging.transaction import Binding, Decision, Request, StoreChecker
from reliable_messaging.transaction_mysql import MySQLTransactionStore

from qs_ai.maintenance.empty_publisher_retirement import retire
from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema
from tests.integration.test_transaction_schema import installed as installed
from tests.integration.test_transaction_schema import plan
from tests.test_empty_publisher_review import reviewed

pytestmark = pytest.mark.integration


def snapshot(conn):
    return {
        t: sorted(repr(tuple(r)) for r in conn.execute(sa.text(f"SELECT * FROM `{t}`")))
        for t in sa.inspect(conn).get_table_names()
        if t not in {"alembic_version", "messaging_outbox"}
    }


@pytest.fixture
async def legacy_engine(installed):
    engine = installed.kw["bind"]
    async with engine.begin() as conn:
        await conn.execute(sa.text("DROP TABLE m3_install_fact"))
    return engine


async def test_fresh_head_install_and_current_metadata_agree(tmp_path):
    url = os.environ["QS_AI_SCHEMA_TEST_SERVER"]
    server = sa.create_engine(url)
    name = "ai_empty_publisher_" + uuid4().hex
    engine = None
    try:
        with server.connect() as conn:
            conn.execute(sa.text(f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4"))
        engine = sa.create_engine(server.url.set(database=name))
        with engine.connect() as conn:
            config = Config("alembic.ini")
            config.attributes["connection"] = conn
            command.upgrade(config, "head")
            conn.commit()
            require_runtime_schema(conn)
            command.check(config)
            assert (
                conn.scalar(sa.text("SELECT version_num FROM alembic_version"))
                == "0044_empty_publisher"
            )
            assert "messaging_outbox" not in sa.inspect(conn).get_table_names()
    finally:
        if engine is not None:
            engine.dispose()
        with server.connect() as conn:
            conn.execute(sa.text(f"DROP DATABASE `{name}`"))
        server.dispose()


async def test_existing_store_without_review_never_changes_native_or_old_rows(legacy_engine):
    engine = legacy_engine
    async with engine.connect() as conn:
        before = await conn.run_sync(snapshot)
        old = (await conn.execute(sa.text("SELECT * FROM messaging_outbox"))).all()
        with pytest.raises(ValueError, match="requires reviewed"):
            await conn.run_sync(
                lambda c: retire(c, fresh=False, review_path=None, reviewed_sha256=None)
            )
        assert await conn.run_sync(snapshot) == before
        assert (await conn.execute(sa.text("SELECT * FROM messaging_outbox"))).all() == old
        assert (
            await conn.scalar(sa.text("SELECT version_num FROM alembic_version"))
            == "0043_native_transaction_limit"
        )


async def test_nonempty_publisher_refuses_after_review_before_consumption(
    legacy_engine, tmp_path, monkeypatch
):
    engine = legacy_engine
    path, candidate = reviewed(tmp_path, monkeypatch)
    async with engine.connect() as conn:
        uuid, db = (await conn.execute(sa.text("SELECT @@server_uuid,DATABASE()"))).one()
        ddl = (await conn.execute(sa.text("SHOW CREATE TABLE messaging_outbox"))).one()[1]
        candidate.update(
            mysql_identity_sha256=hashlib.sha256(
                json.dumps([uuid, db], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            table_ddl_sha256=hashlib.sha256(ddl.encode()).hexdigest(),
        )
        path.write_text(json.dumps(candidate))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        with pytest.raises(ValueError, match="Nonempty publisher"):
            await conn.run_sync(
                lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=digest)
            )
        assert not path.with_suffix(".json.consumed").exists()
        assert await conn.scalar(sa.text("SELECT COUNT(*) FROM messaging_outbox")) == 1
        await conn.run_sync(require_runtime_schema)


async def test_reviewed_upgrade_preserves_all_retained_rows_and_refuses_replay(
    legacy_engine, installed, tmp_path, monkeypatch
):
    engine = legacy_engine
    path, candidate = reviewed(tmp_path, monkeypatch)
    async with engine.begin() as conn:
        # Only the test-owned old publisher fixture is removed to exercise the empty case.
        await conn.execute(sa.text("DELETE FROM messaging_outbox WHERE message_id='old'"))
        await conn.execute(
            sa.text("INSERT INTO rm_tx_binding VALUES('ai.mysql','fixture-primary','m3-1',1)")
        )
        await conn.execute(
            sa.text(
                "INSERT INTO messaging_event_receipts VALUES("
                "'qs-server','retained-ack','retained-event',7,"
                "0x00ff,0x00ff01,1,UTC_TIMESTAMP(6))"
            )
        )
    store = MySQLTransactionStore(installed, Binding("ai.mysql", "fixture-primary", "m3-1"))
    frozen = plan("retained-original-protocol")
    reservation = await store.reserve(
        Request(frozen.owner, frozen.scope, frozen.idempotency_key, frozen.input_hash), frozen
    )
    async with installed() as db:
        await db.begin()
        guard = await store.bind(db, reservation.execution)
        await guard.commit_witness()
        await db.commit()
    async with engine.connect() as conn:
        before = await conn.run_sync(snapshot)
        uuid, db = (await conn.execute(sa.text("SELECT @@server_uuid,DATABASE()"))).one()
        ddl = (await conn.execute(sa.text("SHOW CREATE TABLE messaging_outbox"))).one()[1]
        candidate.update(
            mysql_identity_sha256=hashlib.sha256(
                json.dumps([uuid, db], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            table_ddl_sha256=hashlib.sha256(ddl.encode()).hexdigest(),
        )
        path.write_text(json.dumps(candidate))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        def upgrade(c):
            config = Config("alembic.ini")
            config.attributes["connection"] = c
            config.cmd_opts = type(
                "Options",
                (),
                {"x": [f"empty_publisher_review={path}", f"reviewed_sha256={digest}"]},
            )()
            command.upgrade(config, "head")

        await conn.run_sync(upgrade)
        await conn.commit()
        assert await conn.run_sync(snapshot) == before
        assert (
            await StoreChecker(store).check(reservation.execution.references[0]) == Decision.COMMIT
        )
        await conn.run_sync(require_runtime_schema)
        assert path.with_suffix(".json.consumed").read_text().strip() == digest
        with pytest.raises(ValueError, match="Exact0043"):
            await conn.run_sync(
                lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=digest)
            )
        # Reintroduced table is drift, never an alternate current layout.
        await conn.execute(sa.text("CREATE TABLE messaging_outbox (id INT PRIMARY KEY)"))
        with pytest.raises(ValueError, match="table set"):
            await conn.run_sync(require_runtime_schema)
        await conn.execute(sa.text("DROP TABLE messaging_outbox"))
