"""Full 53-table data round trips on an explicitly disposable MySQL server."""

import hashlib
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.maintenance.schema_refactor import contracts, control, v0038
from qs_ai.maintenance.schema_refactor import conversion as conversion_module
from qs_ai.maintenance.schema_refactor.conversion import copy_data, manifest, verify
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier, qualified
from qs_ai.maintenance.schema_refactor.validation import require_schema

pytestmark = pytest.mark.integration
NOW = datetime(2026, 10, 8, 1, 2, 3, 456789)
UUID = "00000000-0000-0000-0000-000000000001"


def seed(conn):
    """Exercise every original field and dependency, including binary/JSON nulls."""
    persisted = {}
    for table in v0038.metadata.sorted_tables:
        if table.name == "ai_messaging_observations":
            conn.execute(
                sa.text(
                    "UPDATE ai_messaging_observations SET recorded_count=19,"
                    "recording_since=:at,last_observed_at=:at"
                ),
                {"at": NOW},
            )
            continue
        values = {}
        for column in table.columns:
            if column.foreign_keys:
                ref = next(iter(column.foreign_keys)).column
                values[column.name] = persisted[ref.table.name][ref.name]
            elif isinstance(column.type, sa.JSON):
                values[column.name] = {"原始": "😀\n", "large_integer": 9007199254740993}
            elif column.type.python_type is bytes:
                values[column.name] = b"\x00original\xff\n" + "原始😀".encode()
            elif column.type.python_type is datetime:
                values[column.name] = NOW
            elif column.type.python_type is date:
                values[column.name] = date(2026, 10, 8)
            elif column.type.python_type is bool:
                values[column.name] = False
            elif column.type.python_type is int:
                values[column.name] = 1
            elif getattr(column.type, "length", None) == 36:
                values[column.name] = UUID
            elif getattr(column.type, "length", None) == 64:
                values[column.name] = "a" * 64
            elif getattr(column.type, "length", None) == 71:
                values[column.name] = "sha256:" + "a" * 64
            elif "json" in column.name:
                values[column.name] = '{ "原始" : "😀", "untouched" : 1 }\n'
            else:
                values[column.name] = "value"
        if table.name == "evaluation_policy_assets":
            values["kind"] = "execution"
        if table.name == "prompt_assets":
            asset = baseline_assets()[1][0]
            values.update(
                template_id=asset.template_id,
                version=asset.version,
                fingerprint=asset.fingerprint,
                package_sha256=asset.package_sha256,
                package_json=asset.package_json,
            )
        if table.name in ("prompt_draft_revisions", "semantic_draft_versions"):
            values["snapshot_sha256"] = hashlib.sha256(values["snapshot_json"].encode()).hexdigest()
        conn.execute(sa.insert(table).values(**values))
        persisted[table.name] = values
    conn.commit()


@pytest.fixture
def cloned(tmp_path):
    url = os.environ.get("QS_AI_SCHEMA_TEST_SERVER")
    if not url:
        pytest.skip("Requires an explicitly disposable schema-refactor MySQL server")
    engine = sa.create_engine(url)
    suffix = uuid4().hex[:10]
    source, target, archive = (
        "qs_ai_conversion_" + suffix,
        "ai_refactor_" + suffix,
        "ai_backup_" + suffix,
    )
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                f"CREATE DATABASE {identifier(source)} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
        )
        conn.execute(sa.text(f"USE {identifier(source)}"))
        config = Config("alembic.ini")
        config.attributes["connection"] = conn
        command.upgrade(config, OLD_HEAD)
        seed(conn)
        state = control.plan(
            conn, source, target, archive, "sha256:" + "1" * 64, "sha256:" + "2" * 64
        )
        journal = tmp_path / "receipt.json"
        control.save(journal, state)
        try:
            yield conn, state, journal
        finally:
            conn.rollback()
            conn.execute(sa.text("USE mysql"))
            # Only UUID-bound schemas created by this fixture/control are disposable.
            names = [source, target, archive, state.get("rollback_archive")]
            if state.get("rollback_target"):
                names.append(state["rollback_target"])
            for name in names:
                if name:
                    conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(name)}"))
    engine.dispose()


def cutover(conn, state, journal):
    control.prepare(conn, state)
    control.copy(conn, state, True, datetime.now(UTC).isoformat())
    conn.commit()
    control.verified(conn, state, True)
    control.switch(conn, state, True, journal)
    conn.commit()
    control.save(journal, state)


def test_all_53_tables_forward_reverse_and_new_facts(cloned):
    conn, state, journal = cloned
    conn.execute(
        sa.insert(v0038.idempotency),
        [
            dict(
                scope_hash=f"{number:064x}",
                key=f"batch-{number:04}",
                request_hash="b" * 64,
                response={"raw": "原始😀", "number": number},
                created_at=NOW,
            )
            for number in range(501)
        ],
    )
    conn.commit()
    before = manifest(conn, state["source"], OLD_HEAD)
    assert len(before) == 53 and all(entry["rows"] >= 1 for entry in before.values())
    cutover(conn, state, journal)
    require_schema(conn, NEW_HEAD, state["source"])
    assert len(contracts.tables(conn, state["source"])) == 44
    assert manifest(conn, state["source"], NEW_HEAD) == before
    # New data + updates + deletes after cutover must survive the full reverse path.
    conn.execute(
        sa.text(
            f"UPDATE {qualified(state['source'], 'messaging_observations')} "
            "SET recorded_count=recorded_count+7"
        )
    )
    conn.execute(sa.text(f"DELETE FROM {qualified(state['source'], 'execution_model_calls')}"))
    conn.execute(
        sa.text(
            f"INSERT INTO {qualified(state['source'], 'governance_asset_versions')} "
            "(asset_kind,owner_organization_id,asset_id,version,fingerprint,"
            "body_format,body_bytes,source_ref,imported_by) "
            "VALUES ('schema',0,'new-after-cutover','v2',:fingerprint,"
            "'definition_json',:body,'new','new')"
        ),
        {"fingerprint": "sha256:" + "b" * 64, "body": b'{ "new" : true }\n'},
    )
    conn.commit()
    current = manifest(conn, state["source"], NEW_HEAD)
    assert current != before
    control.rollback(conn, state, True, datetime.now(UTC).isoformat(), journal=journal)
    conn.commit()
    require_schema(conn, OLD_HEAD, state["source"])
    assert len(contracts.tables(conn, state["source"])) == 54
    assert manifest(conn, state["source"], OLD_HEAD) == current
    second = "ai_refactor_again_" + uuid4().hex[:8]
    try:
        control.upgrade_isolated(conn, second, NEW_HEAD, state["source"])
        copy_data(conn, state["source"], second, OLD_HEAD, NEW_HEAD)
        assert verify(conn, state["source"], second, OLD_HEAD, NEW_HEAD) == current
    finally:
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(second)}"))


def test_corrupt_body_and_orphan_policy_block_without_source_mutation(cloned):
    conn, state, journal = cloned
    cutover(conn, state, journal)
    original = manifest(conn, state["archive"], OLD_HEAD)
    conn.execute(
        sa.text(
            f"UPDATE {qualified(state['source'], 'governance_asset_versions')} "
            "SET body_bytes=:bad WHERE asset_kind='prompt'"
        ),
        {"bad": b"corrupt"},
    )
    conn.commit()
    with pytest.raises(ValueError, match="mismatch"):
        verify(conn, state["archive"], state["source"], OLD_HEAD, NEW_HEAD)
    assert manifest(conn, state["archive"], OLD_HEAD) == original


def test_fast_rollback_and_retention_guard(cloned):
    conn, state, journal = cloned
    cutover(conn, state, journal)
    with pytest.raises(ValueError, match="30-day"):
        control.cleanup(conn, state)
    control.rollback(conn, state, True, datetime.now(UTC).isoformat(), True, journal=journal)
    conn.commit()
    require_schema(conn, OLD_HEAD, state["source"])
    assert state["phase"] == "rolled_back"


def test_orphan_policy_precheck_preserves_source(cloned):
    conn, state, _ = cloned
    control.prepare(conn, state)
    conn.execute(
        sa.text(
            f"INSERT INTO {qualified(state['source'], 'evaluation_run_policies')} "
            "VALUES (:run,:fingerprint,:body)"
        ),
        {"run": str(uuid4()), "fingerprint": "original", "body": "{}"},
    )
    conn.commit()
    with pytest.raises(ValueError, match="Orphan"):
        control.copy(conn, state, True, datetime.now(UTC).isoformat())
    assert contracts.head(conn, state["source"]) == OLD_HEAD


def test_populated_historical_migrations_preserve_every_field(cloned):
    conn, state, _ = cloned
    before = manifest(conn, state["source"], OLD_HEAD)
    config = Config("alembic.ini")
    config.attributes["connection"] = conn
    command.upgrade(config, NEW_HEAD)
    conn.commit()
    require_schema(conn, NEW_HEAD, state["source"])
    assert len(contracts.tables(conn, state["source"])) == 44
    assert manifest(conn, state["source"], NEW_HEAD) == before


def test_missing_bindings_policies_and_sql_json_null_remain_distinct(cloned):
    conn, state, journal = cloned
    conn.execute(sa.text("DELETE FROM external_requests"))
    conn.execute(sa.text("DELETE FROM evaluation_run_policies"))
    conn.execute(sa.text("UPDATE idempotency_requests SET response=NULL"))
    conn.execute(
        sa.insert(v0038.idempotency).values(
            scope_hash="b" * 64,
            key="json-null",
            request_hash="c" * 64,
            response=sa.JSON.NULL,
            created_at=NOW,
        )
    )
    conn.commit()
    before = manifest(conn, state["source"], OLD_HEAD)
    cutover(conn, state, journal)
    assert manifest(conn, state["source"], NEW_HEAD) == before
    assert (
        conn.scalar(
            sa.text("SELECT COUNT(*) FROM interpretation_sessions WHERE request_id IS NULL")
        )
        == 1
    )
    assert (
        conn.scalar(
            sa.text(
                "SELECT COUNT(*) FROM evaluation_runs WHERE frozen_execution_policy_json IS NULL"
            )
        )
        == 1
    )
    assert conn.execute(
        sa.text(
            "SELECT `key`,response IS NULL FROM interpretation_idempotency_requests ORDER BY `key`"
        )
    ).all() == [("json-null", 0), ("value", 1)]
    control.rollback(conn, state, True, datetime.now(UTC).isoformat(), journal=journal)
    conn.commit()
    assert manifest(conn, state["source"], OLD_HEAD) == before


def test_counter_alignment_resumes_after_implicit_commit_disconnect(cloned, monkeypatch):
    conn, state, journal = cloned
    for table, value in (
        ("evaluation_admission_locks", 9001),
        ("participant_admission_locks", 18001),
    ):
        conn.execute(
            sa.text(f"ALTER TABLE {qualified(state['source'], table)} AUTO_INCREMENT={value}")
        )
    conn.commit()
    control.prepare(conn, state, journal)
    stopped_at = datetime.now(UTC).isoformat()
    native = conversion_module.align_counters

    def partial_then_disconnect(connection, source, target, source_head, target_head, **kwargs):
        connection.execute(
            sa.text(
                f"ALTER TABLE {qualified(target, 'quota_evaluation_admission_locks')} "
                "AUTO_INCREMENT=9001"
            )
        )
        connection.invalidate()
        raise ConnectionError("Lost connection after the first counter's implicit commit")

    monkeypatch.setattr(conversion_module, "align_counters", partial_then_disconnect)
    with pytest.raises(ConnectionError):
        control.copy(conn, state, True, stopped_at, journal)
    conn.rollback()
    state.update(control.read(journal))
    assert state["phase"] == "copy_pending"
    with pytest.raises(ValueError, match="mismatch"):
        verify(conn, state["source"], state["target"], OLD_HEAD, NEW_HEAD)
    monkeypatch.setattr(conversion_module, "align_counters", native)
    control.copy(conn, state, True, stopped_at, journal)
    assert (
        verify(conn, state["source"], state["target"], OLD_HEAD, NEW_HEAD)
        == state["source_manifest"]
    )


def test_version_table_field_drift_is_rejected(cloned):
    conn, state, _ = cloned
    conn.execute(sa.text("ALTER TABLE alembic_version MODIFY version_num VARCHAR(64) NOT NULL"))
    with pytest.raises(ValueError, match="Migration version table"):
        require_schema(conn, OLD_HEAD, state["source"])


def test_owned_migration_cli_commits_final_head_and_preserves_populated_source(cloned):
    conn, state, _ = cloned
    before = manifest(conn, state["source"], OLD_HEAD)
    conn.rollback()  # Release the read snapshot and metadata locks before the CLI's RENAME.
    url = sa.make_url(os.environ["QS_AI_SCHEMA_TEST_SERVER"]).set(
        drivername="mysql+asyncmy", database=state["source"]
    )
    migrated = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        env={**os.environ, "QS_AI_DATABASE_URL": url.render_as_string(hide_password=False)},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert migrated.returncode == 0, migrated.stdout + migrated.stderr
    # No host commit is allowed to mask a missing commit in the independent CLI process.
    assert contracts.head(conn, state["source"]) == NEW_HEAD
    require_schema(conn, NEW_HEAD, state["source"])
    assert len(contracts.tables(conn, state["source"])) == 44
    assert manifest(conn, state["source"], NEW_HEAD) == before


def test_unrepresentable_new_body_cannot_silently_rewrite_legacy_text(cloned):
    conn, state, journal = cloned
    cutover(conn, state, journal)
    invalid = b"raw\xff\x00body"
    conn.execute(
        sa.text(
            f"UPDATE {qualified(state['source'], 'governance_asset_versions')} "
            "SET body_bytes=:body WHERE asset_kind='prompt'"
        ),
        {"body": invalid},
    )
    conn.commit()
    target = "ai_rollback_raw_" + uuid4().hex[:10]
    original_sql_mode = conn.scalar(sa.text("SELECT @@SESSION.sql_mode"))
    try:
        control.upgrade_isolated(conn, target, OLD_HEAD, state["source"])
        # Even a permissive server must not accept a lossy TEXT conversion as equivalent.
        conn.execute(sa.text("SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'"))
        copy_data(conn, state["source"], target, NEW_HEAD, OLD_HEAD)
        with pytest.raises(ValueError, match="mismatch"):
            verify(conn, state["source"], target, NEW_HEAD, OLD_HEAD)
        assert (
            conn.scalar(
                sa.text(
                    "SELECT body_bytes FROM "
                    f"{qualified(state['source'], 'governance_asset_versions')} "
                    "WHERE asset_kind='prompt'"
                )
            )
            == invalid
        )
        assert contracts.head(conn, state["source"]) == NEW_HEAD
    finally:
        conn.execute(sa.text("SET SESSION sql_mode=:mode"), {"mode": original_sql_mode})
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(target)}"))
