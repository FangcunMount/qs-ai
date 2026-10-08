"""Identity attributes are fixed independently of counters and frozen backup hashes."""

import hashlib
import re
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from qs_ai.maintenance.schema_refactor import backup, contracts, v0038, validation
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor.layouts import (
    INTERMEDIATE_HEAD,
    NEW_HEAD,
    OLD_HEAD,
    physical,
)
from qs_ai.maintenance.schema_refactor.validation import _require_auto_increment
from qs_ai.maintenance.schema_refactor.validation_contract import auto_increment_columns

HEADS = (OLD_HEAD, INTERMEDIATE_HEAD, NEW_HEAD)
MISSING = [(head, column) for head in HEADS for column in sorted(auto_increment_columns(head))]


def identity_rows(head, extra="auto_increment"):
    return [(table, column, extra) for table, column in auto_increment_columns(head)]


@pytest.mark.parametrize("head", HEADS)
def test_identity_sidecar_matches_frozen_ddl_without_runtime_metadata(head):
    if head == OLD_HEAD:
        tables = [
            (table.name, str(sa.schema.CreateTable(table).compile(dialect=mysql.dialect())))
            for table in v0038.metadata.tables.values()
        ]
    else:
        tables = [
            (contracts.renamed(table["name"], head), table["create_sql"])
            for table in contracts.load()["tables"]
        ]
    frozen = {
        (table, match[1])
        for table, sql in tables
        for match in re.finditer(
            r"^\s*`?([a-z_][a-z0-9_]*)`?\s+[^\n]*\bAUTO_INCREMENT\b", sql, re.I | re.M
        )
    }
    assert auto_increment_columns(head) == frozen


@pytest.mark.parametrize("head,column", MISSING)
def test_each_missing_identity_attribute_is_rejected(head, column):
    conn = Mock()
    conn.execute.return_value = [row for row in identity_rows(head) if row[:2] != column]
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        _require_auto_increment(conn, "owned_schema", head)


@pytest.mark.parametrize("head", HEADS)
def test_extra_identity_attribute_on_an_existing_column_is_rejected(head):
    conn = Mock()
    conn.execute.return_value = identity_rows(head) + [
        (physical("organization_quota_commands", head), "organization_id", "auto_increment")
    ]
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        _require_auto_increment(conn, "owned_schema", head)


@pytest.mark.parametrize(
    "extra", ["auto_increment", "AUTO_INCREMENT", "DEFAULT_GENERATED auto_increment INVISIBLE"]
)
def test_extra_uses_an_exact_case_insensitive_whitespace_token(extra):
    conn = Mock()
    conn.execute.return_value = identity_rows(NEW_HEAD, extra) + [
        ("evaluation_runs", "run_id", "DEFAULT_GENERATED"),
        ("evaluation_completions", "gen_candidate_key", "VIRTUAL GENERATED"),
    ]
    _require_auto_increment(conn, "owned_schema", NEW_HEAD)
    statement, parameters = conn.execute.call_args.args
    assert "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=:schema" in str(statement)
    assert parameters == {"schema": "owned_schema"}


@pytest.mark.parametrize("extra", ["not_auto_increment", "auto_incremented", "xauto_increment"])
def test_substring_does_not_count_as_an_identity_attribute(extra):
    conn = Mock()
    conn.execute.return_value = identity_rows(NEW_HEAD, extra)
    with pytest.raises(ValueError, match="AUTO_INCREMENT column attributes"):
        _require_auto_increment(conn, "owned_schema", NEW_HEAD)


def test_unknown_head_does_not_query_or_infer_an_identity_contract():
    conn = Mock()
    with pytest.raises(ValueError, match="Unsupported schema head"):
        _require_auto_increment(conn, "owned_schema", "0041_unknown")
    conn.execute.assert_not_called()


@pytest.mark.parametrize("head", [OLD_HEAD, NEW_HEAD])
def test_existing_backup_header_null_counter_rule_remains_compatible(head):
    # The sidecar validates live column attributes, not a new backup format.
    # The existing header rule accepts NULL, including on an expected AI table.
    templates = backup._templates(None, head)
    tables = {}
    for name, spec in templates.items():
        ddl = spec["create_sql"] or str(
            sa.schema.CreateTable(v0038.metadata.tables[name]).compile(dialect=mysql.dialect())
        )
        chars = {
            column: value["collation"] or "utf8mb4_0900_ai_ci"
            for column, value in spec["columns"].items()
            if backup._character(value["type"])
        }
        tables[name] = {
            "columns": sorted(c for c, value in spec["columns"].items() if not value["generated"]),
            "generated": sorted(c for c, value in spec["columns"].items() if value["generated"]),
            "collations": chars,
            "character_sets": {column: value.split("_", 1)[0] for column, value in chars.items()},
            "table_collation": "utf8mb4_0900_ai_ci",
            "table_character_set": "utf8mb4",
            "auto_increment": None,
            "create_sql": ddl,
            "ddl_sha256": hashlib.sha256(ddl.encode()).hexdigest(),
        }
    header = {
        "type": "header",
        "format": fmt.FORMAT,
        "source_schema": "ai",
        "source_server_uuid": "00000000-0000-4000-8000-000000000001",
        "head": head,
        "contract_sha256": backup._contract_sha(head),
        "character_set": "utf8mb4",
        "collation": "utf8mb4_0900_ai_ci",
        "sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
        "tables": tables,
    }
    assert backup._validate_header(header) == templates


@pytest.fixture
def frozen_database(monkeypatch):
    """Full valid 0040 reflection; the sole initial difference is one incoming FK."""
    frozen = {table["name"]: table for table in contracts.load()["tables"]}
    columns, indexes, foreign_keys, checks = {}, {}, {}, {}
    for name, table in frozen.items():
        columns[name] = []
        for column in table["columns"]:
            reflected = {
                "name": column["name"],
                "type": Mock(compile=Mock(return_value=column["type"])),
                "nullable": column["nullable"],
                "default": column["default"],
            }
            if column["computed"]:
                reflected["computed"] = {
                    "sqltext": column["computed"],
                    "persisted": column["persisted"],
                }
            columns[name].append(reflected)
        indexes[name] = [
            {"name": index["name"], "column_names": index["columns"], "unique": index["unique"]}
            for index in table["indexes"]
        ]
        foreign_keys[name] = [
            {
                "name": fk["name"],
                "constrained_columns": fk["columns"],
                "referred_table": fk["table"],
                "referred_columns": fk["referred_columns"],
                "referred_schema": None,
                "options": {"ondelete": fk["ondelete"] or "RESTRICT"},
            }
            for fk in table["foreign_keys"]
        ]
        checks[name] = [{"name": c["name"], "sqltext": c["sql"]} for c in table["checks"]]
    columns["alembic_version"] = [
        {"name": "version_num", "type": sa.String(32), "nullable": False, "default": None}
    ]
    conn = Mock(info={}, dialect=mysql.dialect())
    identity = identity_rows(NEW_HEAD)

    def execute(statement, parameters):
        if "COLUMN_NAME,EXTRA" in str(statement):
            return identity
        return Mock(all=Mock(return_value=[(n, "InnoDB", "BASE TABLE") for n in columns]))

    conn.execute.side_effect = execute
    conn.scalar.side_effect = lambda statement, parameters=None: int(
        "TABLE_SCHEMA<>:schema" in str(statement)
    )
    inspector = Mock()
    inspector.get_columns.side_effect = lambda name, **kw: columns[name]
    inspector.get_pk_constraint.side_effect = lambda name, **kw: {
        "constrained_columns": frozen[name]["primary_key"] if name in frozen else ["version_num"]
    }
    inspector.get_indexes.side_effect = lambda name, **kw: indexes.get(name, [])
    inspector.get_foreign_keys.side_effect = lambda name, **kw: foreign_keys.get(name, [])
    inspector.get_check_constraints.side_effect = lambda name, **kw: checks.get(name, [])
    monkeypatch.setattr(validation.sa, "inspect", lambda connection: inspector)
    monkeypatch.setattr(contracts, "tables", lambda connection, schema: set(columns))
    monkeypatch.setattr(contracts, "head", lambda connection, schema: NEW_HEAD)
    monkeypatch.setattr(
        contracts,
        "legacy_collations",
        lambda *args: {"draft_id": "utf8mb4_0900_ai_ci", "command_id": "utf8mb4_0900_ai_ci"},
    )
    monkeypatch.setattr(
        validation,
        "_effective_collations",
        lambda *args: {
            (name, c["name"]): c["collation"]
            for name, table in frozen.items()
            for c in table["columns"]
            if c["collation"]
        },
    )
    return conn, columns, indexes, foreign_keys, checks, identity


def test_external_incoming_foreign_key_is_rejected_by_default(frozen_database):
    conn, *_ = frozen_database
    with pytest.raises(ValueError, match="External foreign key prevents exchange"):
        validation.require_schema(conn, NEW_HEAD, "owned_schema")


def test_explicit_reference_audit_allows_incoming_while_validating_complete_schema(frozen_database):
    conn, *_ = frozen_database
    validation.require_schema(conn, NEW_HEAD, "owned_schema", allow_external_incoming=True)


def test_reference_audit_does_not_allow_an_outgoing_cross_schema_foreign_key(frozen_database):
    conn, _, _, foreign_keys, _, _ = frozen_database
    foreign_keys["publication_records"][0]["referred_schema"] = "shared_schema"
    with pytest.raises(ValueError, match="Cross-schema foreign key prevents exchange"):
        validation.require_schema(conn, NEW_HEAD, "owned_schema", allow_external_incoming=True)


@pytest.mark.parametrize("damage", ["nullable", "index", "check", "identity"])
def test_reference_audit_keeps_full_structure_and_identity_guards(frozen_database, damage):
    conn, columns, indexes, _, checks, identity = frozen_database
    name = "governance_asset_versions"
    if damage == "nullable":
        columns[name][0]["nullable"] = True
    elif damage == "index":
        indexes[name].pop()
    elif damage == "check":
        checks[name][0]["sqltext"] = "owner_organization_id=0"
    else:
        identity.pop()
    with pytest.raises(ValueError, match="drift"):
        validation.require_schema(conn, NEW_HEAD, "owned_schema", allow_external_incoming=True)
