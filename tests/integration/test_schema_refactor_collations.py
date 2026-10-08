"""Effective legacy collations survive forward and reverse isolated DDL."""

import os
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import DBAPIError

from qs_ai.maintenance.schema_refactor import contracts, v0038
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier, qualified
from qs_ai.maintenance.schema_refactor.validation import _expression, require_schema

pytestmark = pytest.mark.integration


@pytest.fixture
async def server():
    dsn = os.getenv("QS_AI_SCHEMA_TEST_SERVER")
    if not dsn:
        pytest.skip("Requires this run's disposable schema-conversion MySQL service")
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        names = []
        try:
            yield conn, names
        finally:
            for name in reversed(names):
                conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(name)}"))
    engine.dispose()


def create_schema(conn, names, label, collation):
    name = "ai_collation_" + label + "_" + uuid4().hex[:12]
    conn.execute(
        sa.text(f"CREATE DATABASE {identifier(name)} CHARACTER SET utf8mb4 COLLATE {collation}")
    )
    names.append(name)
    return name


def create_old(conn, schema, table_collation):
    current = conn.scalar(sa.text("SELECT DATABASE()"))
    conn.execute(sa.text(f"USE {identifier(schema)}"))
    try:
        for table in v0038.metadata.sorted_tables:
            sql = str(sa.schema.CreateTable(table).compile(dialect=conn.dialect))
            conn.execute(sa.text(sql.rstrip() + " COLLATE " + table_collation))
            for index in table.indexes:
                conn.execute(sa.schema.CreateIndex(index))
        create_head(conn, schema, OLD_HEAD)
    finally:
        conn.execute(sa.text(f"USE {identifier(current)}"))


def create_head(conn, schema, head):
    conn.execute(
        sa.text(
            f"CREATE TABLE {qualified(schema, 'alembic_version')} "
            "(version_num VARCHAR(32) NOT NULL PRIMARY KEY) ENGINE=InnoDB"
        )
    )
    conn.execute(
        sa.text(f"INSERT INTO {qualified(schema, 'alembic_version')} VALUES (:head)"),
        {"head": head},
    )


def require_fresh_runtime_schema(conn, schema):
    # Startup uses its own pool, without the maintenance connection's journal bindings.
    engine = sa.create_engine(conn.engine.url.set(database=schema))
    try:
        with engine.connect() as fresh:
            assert "source_column_collations" not in fresh.info
            assert "legacy_prompt_collations" not in fresh.info
            assert fresh.scalar(sa.text("SELECT DATABASE()")) == schema
            require_schema(fresh, NEW_HEAD)
    finally:
        engine.dispose()


@pytest.mark.parametrize("legacy", ["utf8mb4_general_ci", "utf8mb4_0900_ai_ci"])
async def test_effective_old_columns_survive_database_default_change_both_directions(
    server, legacy
):
    conn, names = server
    source = create_schema(conn, names, "source", legacy)
    create_old(conn, source, legacy)
    original = contracts.source_column_collations(conn, source, OLD_HEAD)
    require_schema(conn, OLD_HEAD, source, column_collations=original)
    other = "utf8mb4_0900_ai_ci" if legacy == "utf8mb4_general_ci" else "utf8mb4_general_ci"
    conn.execute(sa.text(f"ALTER DATABASE {identifier(source)} COLLATE {other}"))
    assert contracts.schema_options(conn, source)[1] == other
    assert contracts.source_column_collations(conn, source, OLD_HEAD) == original
    conn.info["source_column_collations"] = original
    cohort = contracts.legacy_collations(conn, source, OLD_HEAD)
    conn.info["legacy_prompt_collations"] = cohort
    destination = create_schema(conn, names, "new", other)
    contracts.create_final(conn, destination)
    create_head(conn, destination, NEW_HEAD)
    require_schema(conn, NEW_HEAD, destination, column_collations=original)
    assert contracts.legacy_collations(conn, destination, NEW_HEAD) == cohort
    require_fresh_runtime_schema(conn, destination)
    restored = create_schema(conn, names, "restored", other)
    create_old(conn, restored, other)
    contracts.align_legacy_collations(conn, restored, OLD_HEAD, cohort, column_values=original)
    require_schema(conn, OLD_HEAD, restored, column_collations=original)
    assert contracts.source_column_collations(conn, restored, OLD_HEAD) == original


@pytest.mark.parametrize("legacy", ["utf8mb4_general_ci", "utf8mb4_0900_ai_ci"])
async def test_direct_alembic_upgrade_binds_effective_columns_before_database_default_drift(
    server, legacy, monkeypatch
):
    conn, names = server
    source = create_schema(conn, names, "direct", legacy)
    create_old(conn, source, legacy)
    original = contracts.source_column_collations(conn, source, OLD_HEAD)
    cohort = contracts.legacy_collations(conn, source, OLD_HEAD)
    other = "utf8mb4_0900_ai_ci" if legacy == "utf8mb4_general_ci" else "utf8mb4_general_ci"
    conn.execute(sa.text(f"ALTER DATABASE {identifier(source)} COLLATE {other}"))
    native_create = contracts.create_schema

    def tracked_create(connection, destination, origin):
        assert origin == source
        names.append(destination)
        native_create(connection, destination, origin)

    monkeypatch.setattr(contracts, "create_schema", tracked_create)
    current = conn.scalar(sa.text("SELECT DATABASE()"))
    conn.execute(sa.text(f"USE {identifier(source)}"))
    try:
        config = Config("alembic.ini")
        config.attributes["connection"] = conn
        command.upgrade(config, NEW_HEAD)
    finally:
        conn.execute(sa.text(f"USE {identifier(current)}"))
    assert conn.info["source_column_collations"] == original
    assert contracts.legacy_collations(conn, source, NEW_HEAD) == cohort
    require_schema(conn, NEW_HEAD, source, column_collations=original)
    require_fresh_runtime_schema(conn, source)
    # The explicit idx_ definitions replace identical InnoDB-created support
    # indexes; complete shape validation above rejects duplicate access paths.
    indexes = [
        index
        for table in contracts.tables(conn, source) - {"alembic_version"}
        for index in sa.inspect(conn).get_indexes(table, schema=source)
    ]
    assert len(indexes) == sum(len(table["indexes"]) for table in contracts.load()["tables"])
    assert all(index["name"].startswith(("idx_", "uk_")) for index in indexes)


@pytest.mark.parametrize(
    "damage",
    ["default", "foreign_key", "check", "check_disabled", "extra_index", "explicit_collation"],
)
async def test_old_full_ddl_drift_stops_before_copy(server, damage):
    conn, names = server
    source = create_schema(conn, names, "drift", "utf8mb4_general_ci")
    create_old(conn, source, "utf8mb4_general_ci")
    original = contracts.source_column_collations(conn, source, OLD_HEAD)
    if damage == "default":
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(source, 'evaluation_runs')} "
                "ALTER execution_mode SET DEFAULT 'CHANGED'"
            )
        )
    elif damage == "foreign_key":
        fk = sa.inspect(conn).get_foreign_keys("external_requests", schema=source)[0]["name"]
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(source, 'external_requests')} "
                f"DROP FOREIGN KEY {identifier(fk)}"
            )
        )
    elif damage.startswith("check"):
        check = sa.inspect(conn).get_check_constraints("evaluation_policy_assets", schema=source)[
            0
        ]["name"]
        clause = "DROP CHECK" if damage == "check" else "ALTER CHECK"
        suffix = " NOT ENFORCED" if damage == "check_disabled" else ""
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(source, 'evaluation_policy_assets')} "
                f"{clause} {identifier(check)}{suffix}"
            )
        )
    elif damage == "extra_index":
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(source, 'evaluation_runs')} "
                "ADD INDEX unknown_index (requested_by)"
            )
        )
    else:
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(source, 'profile_assets')} "
                "MODIFY fingerprint VARCHAR(71) COLLATE utf8mb4_general_ci NOT NULL"
            )
        )
    with pytest.raises(ValueError):
        require_schema(conn, OLD_HEAD, source, column_collations=original)


async def test_signed_prompt_boundary_preserves_reverse_while_semantic_keeps_u64(server):
    conn, names = server
    target = create_schema(conn, names, "boundary", "utf8mb4_0900_ai_ci")
    contracts.create_final(conn, target)
    heads = qualified(target, "governance_draft_heads")
    versions = qualified(target, "governance_draft_versions")
    maximum = 2**63 - 1
    identity = str(uuid4())
    conn.execute(
        sa.text(
            f"INSERT INTO {heads} (draft_kind,organization_id,draft_id,revision) "
            "VALUES ('prompt',:owner,:id,1)"
        ),
        {"owner": maximum, "id": identity},
    )
    row_id = conn.scalar(sa.text(f"SELECT draft_row_id FROM {heads} WHERE draft_kind='prompt'"))
    prompt_version_sql = sa.text(
        f"INSERT INTO {versions} "
        "(draft_row_id,revision,draft_kind,organization_id,operator_user_id,command_id,"
        "request_bytes,snapshot_bytes,snapshot_sha256) "
        "VALUES (:row,1,'prompt',:organization_id,:operator_user_id,:command,'{}','{}',:hash)"
    )
    for field in ("organization_id", "operator_user_id"):
        values = {"organization_id": maximum, "operator_user_id": maximum}
        values[field] += 1
        with pytest.raises(DBAPIError):
            conn.execute(
                prompt_version_sql,
                {**values, "row": row_id, "command": str(uuid4()), "hash": "a" * 64},
            )
    conn.execute(
        prompt_version_sql,
        {
            "organization_id": maximum,
            "operator_user_id": maximum,
            "row": row_id,
            "command": str(uuid4()),
            "hash": "a" * 64,
        },
    )
    with pytest.raises(DBAPIError):
        conn.execute(
            sa.text(
                f"INSERT INTO {heads} (draft_kind,organization_id,draft_id,revision) "
                "VALUES ('prompt',:owner,:id,1)"
            ),
            {"owner": maximum + 1, "id": str(uuid4())},
        )
    conn.execute(
        sa.text(
            f"INSERT INTO {heads} (draft_kind,organization_id,draft_id,revision) "
            "VALUES ('semantic',:owner,:id,1)"
        ),
        {"owner": 2**64 - 1, "id": str(uuid4())},
    )
    row_id = conn.scalar(sa.text(f"SELECT draft_row_id FROM {heads} WHERE draft_kind='semantic'"))
    conn.execute(
        sa.text(
            f"INSERT INTO {versions} "
            "(draft_row_id,revision,draft_kind,organization_id,snapshot_bytes,snapshot_sha256) "
            "VALUES (:row,1,'semantic',:owner,'{}',:hash)"
        ),
        {"row": row_id, "owner": 2**64 - 1, "hash": "a" * 64},
    )


def test_expression_equivalence_preserves_boolean_grouping_and_literal_case():
    assert _expression("(a OR b) AND c") != _expression("a OR (b AND c)")
    assert _expression("kind='profile'") != _expression("kind='PROFILE'")
    assert _expression("((a = 1) AND (b IS NULL))") == _expression("a=1 AND b IS NULL")
