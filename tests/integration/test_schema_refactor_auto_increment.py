"""Identity DDL drift is rejected by shared gates on disposable MySQL 8.0/8.4."""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.bootstrap import database_check
from qs_ai.bootstrap.server import preflight as startup_preflight
from qs_ai.maintenance.schema_refactor import backup, contracts
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor.layouts import (
    INTERMEDIATE_HEAD,
    NEW_HEAD,
    OLD_HEAD,
    identifier,
    physical,
    qualified,
)
from qs_ai.maintenance.schema_refactor.validation import require_schema
from qs_ai.maintenance.schema_refactor.validation_contract import auto_increment_columns

pytestmark = pytest.mark.integration
HEADS = (OLD_HEAD, INTERMEDIATE_HEAD, NEW_HEAD)
MISSING = [(head, column) for head in HEADS for column in sorted(auto_increment_columns(head))]


@pytest.fixture
def identity_server():
    dsn = os.getenv("QS_AI_SCHEMA_TEST_SERVER")
    if not dsn:
        pytest.skip("Requires an explicitly disposable schema-refactor MySQL server")
    engine = sa.create_engine(dsn)
    names = []
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SET SESSION information_schema_stats_expiry=0"))
            conn.commit()
            try:
                yield conn, names
            finally:
                conn.rollback()
                conn.execute(sa.text("USE mysql"))
                for name in reversed(names):
                    conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(name)}"))
                conn.commit()
    finally:
        engine.dispose()


def create_layout(server, head):
    conn, names = server
    schema = "ai_identity_" + uuid4().hex[:12]
    names.append(schema)
    conn.execute(
        sa.text(
            f"CREATE DATABASE {identifier(schema)} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        )
    )
    conn.execute(sa.text(f"USE {identifier(schema)}"))
    if head == OLD_HEAD:
        config = Config("alembic.ini")
        config.attributes["connection"] = conn
        command.upgrade(config, OLD_HEAD)
    else:
        contracts.create_final(conn, schema, head)
        conn.execute(
            sa.text(
                "CREATE TABLE alembic_version "
                "(version_num VARCHAR(32) NOT NULL PRIMARY KEY) ENGINE=InnoDB"
            )
        )
        conn.execute(sa.text("INSERT INTO alembic_version VALUES (:head)"), {"head": head})
    conn.commit()
    require_schema(conn, head, schema)
    conn.commit()
    return schema


def remove_identity(conn, schema, column):
    table, name = column
    # MySQL rejects even an attribute-only MODIFY on a referenced parent column.
    # Remove just its incoming FK in this UUID-owned test schema, then reconstruct
    # that exact relationship before invoking the production schema validator.
    incoming = conn.execute(
        sa.text(
            "SELECT TABLE_NAME,CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA=:schema AND REFERENCED_TABLE_SCHEMA=:schema "
            "AND REFERENCED_TABLE_NAME=:table AND REFERENCED_COLUMN_NAME=:column"
        ),
        {"schema": schema, "table": table, "column": name},
    ).all()
    foreign_keys = []
    inspector = sa.inspect(conn)
    for child, constraint in incoming:
        fk = next(
            f for f in inspector.get_foreign_keys(child, schema=schema) if f["name"] == constraint
        )
        foreign_keys.append((child, fk))
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(schema, child)} DROP FOREIGN KEY {identifier(constraint)}"
            )
        )
    try:
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(schema, table)} "
                f"MODIFY {identifier(name)} BIGINT UNSIGNED NOT NULL"
            )
        )
    finally:
        for child, fk in foreign_keys:
            fields = ",".join(identifier(c) for c in fk["constrained_columns"])
            references = ",".join(identifier(c) for c in fk["referred_columns"])
            actions = "".join(
                f" {clause} {fk['options'][option]}"
                for option, clause in (("ondelete", "ON DELETE"), ("onupdate", "ON UPDATE"))
                if option in fk.get("options", {})
            )
            conn.execute(
                sa.text(
                    f"ALTER TABLE {qualified(schema, child)} ADD CONSTRAINT "
                    f"{identifier(fk['name'])} FOREIGN KEY ({fields}) REFERENCES "
                    f"{qualified(schema, fk['referred_table'])} ({references}){actions}"
                )
            )
            assert sa.inspect(conn).get_foreign_keys(
                child, schema=schema
            ) == inspector.get_foreign_keys(child, schema=schema)
    conn.commit()


def grow_counters(conn, schema, head):
    for table, column in sorted(auto_increment_columns(head)):
        conn.execute(sa.text(f"ALTER TABLE {qualified(schema, table)} AUTO_INCREMENT=5000"))
        values = {column: None}
        if table == "governance_asset_versions":
            values.update(
                asset_kind="profile",
                asset_id="identity-growth",
                version="v1",
                fingerprint="sha256:" + "a" * 64,
                body_format="definition_json",
                body_bytes=b"{}",
                source_ref="identity-test",
                imported_by="identity-test",
            )
        elif table == "governance_draft_heads":
            values.update(
                draft_kind="semantic",
                organization_id=17,
                draft_id=str(uuid4()),
                revision=0,
            )
        fields = ",".join(identifier(field) for field in values)
        parameters = ",".join(":" + field for field in values)
        conn.execute(
            sa.text(f"INSERT INTO {qualified(schema, table)} ({fields}) VALUES ({parameters})"),
            values,
        )
        assert conn.scalar(sa.text("SELECT LAST_INSERT_ID()")) == 5000
    conn.commit()


def counters(conn, schema):
    return {
        table: value
        for table, value in conn.execute(
            sa.text(
                "SELECT TABLE_NAME,AUTO_INCREMENT FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=:schema AND AUTO_INCREMENT IS NOT NULL"
            ),
            {"schema": schema},
        )
    }


@pytest.mark.parametrize("head,column", MISSING)
def test_missing_each_auto_increment_attribute_blocks_exact_schema(identity_server, head, column):
    conn, _ = identity_server
    schema = create_layout(identity_server, head)
    remove_identity(conn, schema, column)
    # MySQL's native reflected flag changes while type, NULL, PK and indexes stay fixed.
    reflected = next(
        c for c in sa.inspect(conn).get_columns(column[0], schema=schema) if c["name"] == column[1]
    )
    assert not reflected.get("autoincrement", False)
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        require_schema(conn, head, schema)


@pytest.mark.parametrize("head", HEADS)
def test_extra_auto_increment_on_existing_composite_key_is_rejected(identity_server, head):
    conn, _ = identity_server
    schema = create_layout(identity_server, head)
    table = physical("organization_quota_commands", head)
    before = sa.inspect(conn)
    columns = before.get_columns(table, schema=schema)
    pk = before.get_pk_constraint(table, schema=schema)
    indexes = before.get_indexes(table, schema=schema)
    conn.execute(
        sa.text(
            f"ALTER TABLE {qualified(schema, table)} "
            "MODIFY organization_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"
        )
    )
    conn.commit()
    after = sa.inspect(conn)
    assert after.get_pk_constraint(table, schema=schema) == pk
    assert after.get_indexes(table, schema=schema) == indexes
    for old, new in zip(columns, after.get_columns(table, schema=schema), strict=True):
        assert str(old["type"]) == str(new["type"])
        assert old["nullable"] == new["nullable"]
        assert old.get("default") == new.get("default")
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        require_schema(conn, head, schema)


@pytest.mark.parametrize("head", HEADS)
def test_legal_floor_and_native_counter_growth_do_not_change_identity_contract(
    identity_server, head
):
    conn, _ = identity_server
    schema = create_layout(identity_server, head)
    grow_counters(conn, schema, head)
    assert counters(conn, schema) == {table: 5001 for table, _ in auto_increment_columns(head)}
    require_schema(conn, head, schema)


def test_readonly_preflight_and_backup_fail_before_artifact_publication(identity_server, tmp_path):
    conn, _ = identity_server
    schema = create_layout(identity_server, NEW_HEAD)
    remove_identity(conn, schema, ("governance_asset_versions", "asset_row_id"))
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        backup.preflight(conn, schema)
    conn.rollback()
    path = tmp_path / "not-published.jsonl.gz"
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        backup.backup(conn, schema, path)
    assert not path.exists()
    assert not fmt.receipt_path(path).exists()


async def test_startup_and_readonly_release_check_share_the_identity_gate(
    identity_server, monkeypatch
):
    conn, _ = identity_server
    schema = create_layout(identity_server, NEW_HEAD)
    remove_identity(conn, schema, ("governance_draft_heads", "draft_row_id"))
    url = conn.engine.url.set(database=schema, drivername="mysql+asyncmy")
    engine = create_async_engine(url)
    container = Mock()
    container.get = AsyncMock(return_value=SimpleNamespace(engine=engine))
    settings = SimpleNamespace()
    try:
        with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
            await startup_preflight(container, settings)
    finally:
        await engine.dispose()
    monkeypatch.setenv("QS_AI_DATABASE_URL", url.render_as_string(hide_password=False))
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        await database_check.check(require_head=True)


@pytest.mark.parametrize("head", [OLD_HEAD, NEW_HEAD])
def test_v1_frozen_contract_backup_still_restores_counters_and_identity_attributes(
    identity_server, tmp_path, head
):
    conn, names = identity_server
    schema = create_layout(identity_server, head)
    grow_counters(conn, schema, head)
    before = counters(conn, schema)
    conn.commit()
    path = tmp_path / "compatible-v1.jsonl.gz"
    receipt = backup.backup(conn, schema, path, expected_head=head)
    inspected = backup.inspect_backup(schema, path, expected_sha256=receipt["backup_sha256"])
    assert inspected["format"] == "qs-ai-native-backup/v1"
    assert inspected["verified"]
    target = "ai_refactor_identity_" + uuid4().hex[:12]
    names.append(target)
    restored = backup.restore(conn, schema, path, target, receipt["backup_sha256"])
    assert restored["verified"]
    assert restored["physical_manifest_sha256"] == receipt["physical_manifest_sha256"]
    require_schema(conn, head, target, column_collations={})
    assert counters(conn, target) == before
