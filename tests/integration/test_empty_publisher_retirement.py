"""Real MySQL only: exact-head empty-table retirement and protected native facts."""

import hashlib
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from reliable_messaging.transaction import Binding, Decision, Request, StoreChecker
from reliable_messaging.transaction_mysql import MySQLTransactionStore

from qs_ai.maintenance.empty_publisher_retirement import retire
from qs_ai.maintenance.publisher_material import snapshot as protected_snapshot
from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema
from tests.integration.test_initial_transaction_binding import invoke
from tests.integration.test_transaction_schema import installed as installed
from tests.integration.test_transaction_schema import plan
from tests.test_empty_publisher_review import bind_review, reviewed, write_json

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
        target = SimpleNamespace(url=engine.url.set(drivername="mysql+asyncmy"))
        review = tmp_path / "initial-binding.json"
        preview = await invoke(
            target,
            review,
            "preview",
            "--operation-id",
            "fresh0044",
            "--store-id",
            "new-original-primary",
            "--writer-epoch",
            "m3-1",
        )
        assert preview[0] == 0, preview[2]
        assert json.loads(review.read_text())["schema_head"] == "0044_empty_publisher"
        approved = hashlib.sha256(review.read_bytes()).hexdigest()
        result = await invoke(
            target, review, "apply", "--reviewed-sha256", approved, "--writers-excluded"
        )
        assert result[0] == 0, result[2]
        verified = await invoke(target, review, "verify", "--reviewed-sha256", approved)
        assert verified[0] == 0 and verified[1]["read_only"] is True
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
        frozen_snapshot = await conn.run_sync(protected_snapshot)
        digest = bind_review(path, candidate, frozen_snapshot)
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
        frozen_snapshot = await conn.run_sync(protected_snapshot)
        digest = bind_review(path, candidate, frozen_snapshot)

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


async def test_pre_drop_failure_keeps_consumed_marker_and_original_store(
    legacy_engine, tmp_path, monkeypatch
):
    engine = legacy_engine
    path, candidate = reviewed(tmp_path, monkeypatch)
    async with engine.begin() as conn:
        await conn.execute(sa.text("DELETE FROM messaging_outbox WHERE message_id='old'"))
    async with engine.connect() as conn:
        before = await conn.run_sync(snapshot)
        frozen_snapshot = await conn.run_sync(protected_snapshot)
        digest = bind_review(path, candidate, frozen_snapshot)

        def fail_drop(connection, cursor, statement, parameters, context, executemany):
            if statement == "DROP TABLE messaging_outbox":
                raise RuntimeError("injected before DDL dispatch")

        sa.event.listen(engine.sync_engine, "before_cursor_execute", fail_drop)
        try:
            with pytest.raises(RuntimeError, match="before DDL dispatch"):
                await conn.run_sync(
                    lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=digest)
                )
        finally:
            sa.event.remove(engine.sync_engine, "before_cursor_execute", fail_drop)
        assert path.with_suffix(".json.consumed").read_text().strip() == digest
        assert await conn.run_sync(snapshot) == before
        await conn.run_sync(require_runtime_schema)
        with pytest.raises(FileExistsError):
            await conn.run_sync(
                lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=digest)
            )
        assert await conn.scalar(sa.text("SELECT COUNT(*) FROM messaging_outbox")) == 0


def forward_review(path, candidate, original_sha):
    from datetime import UTC, datetime, timedelta

    frozen = json.loads((path.parent / "protection.json").read_text())
    value = {
        "schema": "ai-publisher-forward-review/v1",
        "source_sha": candidate["source_sha"],
        "operation_id": candidate["operation_id"],
        "mysql_identity_sha256": candidate["mysql_identity_sha256"],
        "original_review": str(path),
        "original_sha256": original_sha,
        "writer_receipt": frozen["writer_receipt"],
        "writer_sha256": frozen["writer_sha256"],
        "data_owner": "shujie",
        "recovery_owner": "shujie",
        "recorded_at": datetime.now(UTC).isoformat(),
        "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
    }
    recovery = path.parent / "forward.json"
    return recovery, value


async def test_unknown_ddl_result_converges_forward_without_replaying_drop(
    legacy_engine,
    tmp_path,
    monkeypatch,
):
    from qs_ai.maintenance.publisher_material import same_retained
    from qs_ai.maintenance.publisher_window import reconcile

    engine = legacy_engine
    path, candidate = reviewed(tmp_path, monkeypatch)
    async with engine.begin() as conn:
        await conn.execute(sa.text("DELETE FROM messaging_outbox WHERE message_id='old'"))
        await conn.execute(
            sa.text("INSERT INTO rm_tx_binding VALUES('ai.mysql','original-primary','m3-1',1)")
        )
        await conn.execute(
            sa.text(
                "INSERT INTO messaging_event_receipts VALUES("
                "'qs-server','retained-ack','retained-event',7,"
                "0x00ff,0x00ff01,1,UTC_TIMESTAMP(6))"
            )
        )
    async with engine.connect() as conn:
        before = await conn.run_sync(protected_snapshot)
        approved = bind_review(path, candidate, before)
        # Real MySQL DDL commits; emulate lost connection before Alembic updates its ledger.
        await conn.run_sync(
            lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=approved)
        )
        await conn.commit()
        with pytest.raises(ValueError, match="table set"):
            await conn.run_sync(require_runtime_schema)
        pending = await conn.run_sync(protected_snapshot)
        assert pending["state"] == "43_drop_committed_ledger_pending"
        same_retained(before, pending)
        with pytest.raises(ValueError):
            await conn.run_sync(
                lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=approved)
            )
        await conn.rollback()
        recovery, value = forward_review(path, candidate, approved)
        recovery_sha = write_json(recovery, value)
        statements = []

        def record(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        sa.event.listen(engine.sync_engine, "before_cursor_execute", record)
        try:
            result = await conn.run_sync(lambda c: reconcile(c, str(recovery), recovery_sha))
            await conn.commit()
        finally:
            sa.event.remove(engine.sync_engine, "before_cursor_execute", record)
        assert result["DDL_executed"] is False
        assert len([s for s in statements if s.startswith("UPDATE ")]) == 1
        assert not any(s.startswith(("DROP ", "CREATE ", "DELETE ", "INSERT ")) for s in statements)
        after = await conn.run_sync(protected_snapshot)
        assert after["state"] == "44_complete" and after["target"]["present"] is False
        same_retained(before, after)
        await conn.run_sync(require_runtime_schema)
        with pytest.raises(ValueError, match="exact43 pending"):
            await conn.run_sync(lambda c: reconcile(c, str(recovery), recovery_sha))
        assert recovery.with_suffix(".json.consumed").read_text().strip() == recovery_sha


@pytest.mark.parametrize(
    "drift",
    [
        "retained_bytes",
        "retained_schema",
        "missing_original_marker",
        "wrong_database",
        "business_reopened",
    ],
)
async def test_forward_recovery_refuses_unproved_original_state(
    legacy_engine,
    tmp_path,
    monkeypatch,
    drift,
):
    from qs_ai.maintenance.publisher_window import reconcile

    engine = legacy_engine
    path, candidate = reviewed(tmp_path, monkeypatch)
    async with engine.begin() as conn:
        await conn.execute(sa.text("DELETE FROM messaging_outbox WHERE message_id='old'"))
        await conn.execute(
            sa.text("INSERT INTO rm_tx_binding VALUES('ai.mysql','original-primary','m3-1',1)")
        )
        await conn.execute(
            sa.text(
                "INSERT INTO messaging_event_receipts VALUES("
                "'qs-server','retained-ack','retained-event',7,"
                "0x00ff,0x00ff01,1,UTC_TIMESTAMP(6))"
            )
        )
    async with engine.connect() as conn:
        before = await conn.run_sync(protected_snapshot)
        approved = bind_review(path, candidate, before)
        await conn.run_sync(
            lambda c: retire(c, fresh=False, review_path=str(path), reviewed_sha256=approved)
        )
        await conn.commit()
        recovery, value = forward_review(path, candidate, approved)
        if drift == "retained_bytes":
            await conn.execute(sa.text("UPDATE rm_tx_binding SET writer_epoch='drift'"))
            await conn.commit()
        elif drift == "retained_schema":
            await conn.execute(sa.text("CREATE TABLE unexpected_retained_drift (id INT)"))
        elif drift == "missing_original_marker":
            path.with_suffix(".json.consumed").unlink()
        elif drift == "business_reopened":
            write_json(path.with_suffix(".json.business-reopen-issued"), {"issued": True})
        else:
            value["mysql_identity_sha256"] = "0" * 64
        recovery_sha = write_json(recovery, value)
        with pytest.raises((ValueError, FileNotFoundError)):
            await conn.run_sync(lambda c: reconcile(c, str(recovery), recovery_sha))
        assert not recovery.with_suffix(".json.consumed").exists()
        assert (
            await conn.scalar(sa.text("SELECT version_num FROM alembic_version"))
            == "0043_native_transaction_limit"
        )
        assert "messaging_outbox" not in await conn.run_sync(
            lambda c: sa.inspect(c).get_table_names()
        )


def test_original_production_empty_target_ddl_restores_only_in_isolated_mysql():
    from pathlib import Path

    server = sa.create_engine(os.environ["QS_AI_SCHEMA_TEST_SERVER"])
    name = "ai_target_restore_" + uuid4().hex
    restored = None
    original = Path("tests/fixtures/publisher_retirement/production-empty-target.sql").read_text()
    assert (
        hashlib.sha256(original.encode()).hexdigest()
        == "a4e7e09941d249faa1a2a4c100eadbdd53c3be42181a148c74ba8c256cb508ff"
    )
    try:
        with server.connect() as conn:
            conn.execute(sa.text(f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4"))
        restored = sa.create_engine(server.url.set(database=name))
        with restored.connect() as conn:
            conn.execute(sa.text(original))
            assert conn.scalar(sa.text("SELECT COUNT(*) FROM messaging_outbox")) == 0
            assert (
                str(conn.execute(sa.text("SHOW CREATE TABLE messaging_outbox")).one()[1])
                == original
            )
            assert sa.inspect(conn).get_table_names() == ["messaging_outbox"]
    finally:
        if restored is not None:
            restored.dispose()
        with server.connect() as conn:
            conn.execute(sa.text(f"DROP DATABASE `{name}`"))
        server.dispose()


async def test_capture_cli_is_readonly_and_releases_owned_pool(
    legacy_engine, tmp_path, monkeypatch
):
    from qs_ai.maintenance.publisher_window import run

    engine = legacy_engine
    monkeypatch.setenv("QS_AI_RELEASE_SHA", "a" * 40)
    monkeypatch.setenv(
        "QS_AI_PUBLISHER_WINDOW_DATABASE_URL", engine.url.render_as_string(hide_password=False)
    )
    output = tmp_path / "live.json"
    async with engine.connect() as conn:
        before = await conn.run_sync(protected_snapshot)
    result = await run(
        SimpleNamespace(
            command="capture",
            operation_id="native-readonly",
            writer_receipt=None,
            writer_sha256=None,
            output=str(output),
        )
    )
    assert result == 0
    value = json.loads(output.read_text())
    assert value["frozen"] is False and value["execution_approval"] is False
    assert value["pool_released"] is True and value["transaction_released"] is True
    assert value["snapshot"]["target"]["rows"] == 1  # Never fake the nonempty legacy fixture.
    async with engine.connect() as conn:
        after = await conn.run_sync(protected_snapshot)
    assert before["retained"] == after["retained"]
    assert before["target"] == after["target"] and before["head"] == after["head"]
