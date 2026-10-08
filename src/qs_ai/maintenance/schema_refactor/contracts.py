"""Versioned DDL and structural signatures, independent of runtime metadata."""

import json
import re
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.schema_refactor import v0038
from qs_ai.maintenance.schema_refactor.layouts import (
    INTERMEDIATE_HEAD,
    MERGES,
    NEW_HEAD,
    OLD_HEAD,
    RENAMES,
    identifier,
    physical,
    qualified,
)

ColumnCollations = dict[str, dict[str, str]]


def _known_character_columns() -> dict[str, set[str]]:
    return {
        table.name: {column.name for column in table.columns if isinstance(column.type, sa.String)}
        for table in v0038.metadata.tables.values()
    }


def validate_column_collations(values: ColumnCollations) -> ColumnCollations:
    """Only frozen legacy fields may influence DDL; explicit collations stay fixed."""
    known = _known_character_columns()
    if set(values) != set(known):
        raise ValueError("Inherited collation table map is incomplete or unknown")
    for table, columns in known.items():
        if set(values[table]) != columns:
            raise ValueError("Inherited collation column map is incomplete or unknown: " + table)
        for name, value in values[table].items():
            declared = getattr(v0038.metadata.tables[table].c[name].type, "collation", None)
            if not isinstance(value, str) or not re.fullmatch(
                r"(?:utf8mb4|ascii)_[a-z0-9_]+", value
            ):
                raise ValueError("Unsupported inherited character collation")
            if declared and value != declared:
                raise ValueError("Legacy explicit collation has drifted: " + table + "." + name)
            if not declared and not value.startswith("utf8mb4_"):
                raise ValueError("Legacy inherited character set has drifted")
    return {table: dict(columns) for table, columns in values.items()}


def source_column_collations(
    conn: Connection,
    schema: str,
    head: str = OLD_HEAD,
) -> ColumnCollations:
    """Bind actual legacy columns, independently of the current database default.

    A consolidated source cannot reconstruct type-specific old TEXT collations.
    Reverse conversion therefore requires the original forward journal binding.
    """
    if head != OLD_HEAD:
        original = conn.info.get("source_column_collations")
        if not original:
            raise ValueError("Reverse conversion requires original source column collations")
        return validate_column_collations(original)
    rows = conn.execute(
        sa.text(
            "SELECT TABLE_NAME,COLUMN_NAME,COLLATION_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=:schema AND COLLATION_NAME IS NOT NULL"
        ),
        {"schema": schema},
    ).all()
    by_column = {(table, name): value for table, name, value in rows}
    values = {
        table: {name: by_column.get((table, name), "") for name in columns}
        for table, columns in _known_character_columns().items()
    }
    return validate_column_collations(values)


def _cohort(values: ColumnCollations, columns: list[tuple[str, str]]) -> str:
    collations = {values[table][column] for table, column in columns}
    if len(collations) != 1:
        raise ValueError("Merged legacy comparison cohort has drifted: " + repr(columns))
    return collations.pop()


def final_column_collations(
    values: ColumnCollations | None,
    prompt_values: dict[str, str] | None = None,
) -> dict[tuple[str, str], str]:
    """Render retained fields and representable merged comparison cohorts only."""
    result = {}
    if values:
        values = validate_column_collations(values)
        for table, columns in values.items():
            if table not in MERGES:
                result.update(
                    {(physical(table, NEW_HEAD), name): value for name, value in columns.items()}
                )
        prompt_values = {
            kind: _cohort(values, columns) for kind, columns in LEGACY_COLUMNS[OLD_HEAD].items()
        }
        result[("interpretation_sessions", "request_id")] = values["external_requests"][
            "request_id"
        ]
        _cohort(values, [("evaluation_runs", "run_id"), ("evaluation_run_policies", "run_id")])
        for old, new in (
            ("fingerprint", "frozen_execution_policy_fingerprint"),
            ("definition_json", "frozen_execution_policy_json"),
        ):
            result[("evaluation_runs", new)] = values["evaluation_run_policies"][old]
        result[("governance_draft_versions", "snapshot_sha256")] = _cohort(
            values,
            [
                ("prompt_draft_revisions", "snapshot_sha256"),
                ("semantic_draft_versions", "snapshot_sha256"),
            ],
        )
        for column in ("run_id", "execution_id", "invocation_id", "candidate_id"):
            result[("evaluation_completions", column)] = _cohort(
                values,
                [
                    ("evaluation_generation_completions", column),
                    ("evaluation_semantic_completions", column),
                ],
            )
        result[("evaluation_completions", "case_id")] = values["evaluation_generation_completions"][
            "case_id"
        ]
    for kind, cohort_columns in LEGACY_COLUMNS[NEW_HEAD].items():
        if prompt_values and kind in prompt_values:
            result.update({pair: prompt_values[kind] for pair in cohort_columns})
    return result


def _render_column(sql: str, column: str, collation: str) -> str:
    if not re.fullmatch(r"(?:utf8mb4|ascii)_[a-z0-9_]+", collation):
        raise ValueError("Invalid inherited collation")
    pattern = rf"^(\s*`?{re.escape(column)}`?\s+)([^\n]+)$"

    def render(match: re.Match[str]) -> str:
        definition = re.sub(r"\s+COLLATE \w+", "", match[2], flags=re.I)
        definition = re.sub(r"\s+CHARACTER SET \w+", "", definition, flags=re.I)
        typed = re.match(
            r"((?:VARCHAR|CHAR|TINYTEXT|MEDIUMTEXT|LONGTEXT|TEXT)(?:\(\d+\))?)(.*)",
            definition,
            re.I,
        )
        if typed is None:
            raise ValueError("Inherited collation cannot modify a new business type")
        charset = collation.split("_", 1)[0]
        return (
            match[1] + typed[1] + " CHARACTER SET " + charset + " COLLATE " + collation + typed[2]
        )

    result, count = re.subn(pattern, render, sql, flags=re.M)
    if count != 1:
        raise ValueError("Unknown inherited DDL column: " + column)
    return result


LEGACY_COLUMNS = {
    OLD_HEAD: {
        "draft_id": [
            ("prompt_drafts", "draft_id"),
            ("prompt_draft_revisions", "draft_id"),
            ("prompt_draft_freezes", "draft_id"),
            ("solution_revisions", "draft_id"),
        ],
        "command_id": [("prompt_draft_revisions", "command_id")],
    },
    NEW_HEAD: {
        "draft_id": [
            ("governance_draft_heads", "prompt_draft_id_key"),
            ("governance_prompt_draft_freezes", "draft_id"),
            ("governance_solutions", "draft_id"),
        ],
        "command_id": [
            ("governance_draft_versions", "command_id"),
            ("governance_draft_versions", "prompt_command_id_key"),
        ],
    },
}


def legacy_collations(conn: Connection, schema: str, head: str) -> dict[str, str]:
    groups = LEGACY_COLUMNS[OLD_HEAD if head == OLD_HEAD else NEW_HEAD]
    result = {}
    for kind, columns in groups.items():
        values = []
        for table, column in columns:
            if head == INTERMEDIATE_HEAD:
                table = renamed(table, head)
            value = conn.scalar(
                sa.text(
                    "SELECT COLLATION_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=:schema AND TABLE_NAME=:table AND COLUMN_NAME=:column"
                ),
                {"schema": schema, "table": table, "column": column},
            )
            if not value or not re.fullmatch(r"utf8mb4_[a-z0-9_]+", value):
                raise ValueError("Unsupported legacy prompt collation")
            values.append(value)
        if len(set(values)) != 1:
            raise ValueError("Legacy prompt comparison cohort has drifted")
        result[kind] = values[0]
    return result


def load() -> dict[str, Any]:
    return json.loads(Path(__file__).with_name("v0040.json").read_text())


def renamed(sql: str, head: str) -> str:
    if head == INTERMEDIATE_HEAD:
        for old, new in sorted(RENAMES.items(), key=lambda item: -len(item[1])):
            sql = re.sub(rf"(?<![a-z0-9_]){re.escape(new)}(?![a-z0-9_])", old, sql)
    return sql


def create_final(conn: Connection, schema: str, head: str = NEW_HEAD) -> None:
    current = conn.scalar(sa.text("SELECT DATABASE()"))
    conn.execute(sa.text(f"USE {identifier(schema)}"))
    try:
        inherited = final_column_collations(
            conn.info.get("source_column_collations"),
            conn.info.get("legacy_prompt_collations"),
        )
        for table in load()["tables"]:
            sql = table["create_sql"]
            for (table_name, column), value in inherited.items():
                if table["name"] == table_name:
                    sql = _render_column(sql, column, value)
            conn.execute(sa.text(renamed(sql, head)))
            for statement in table["index_sql"]:
                conn.execute(sa.text(renamed(statement, head)))
    finally:
        if current:
            conn.execute(sa.text(f"USE {identifier(current)}"))


def align_legacy_collations(
    conn: Connection,
    schema: str,
    head: str,
    values: dict[str, str],
    *,
    column_values: ColumnCollations | None = None,
) -> None:
    if head != OLD_HEAD:
        return
    desired = (
        validate_column_collations(column_values)
        if column_values
        else {table: {} for table in v0038.metadata.tables}
    )
    for kind, columns in LEGACY_COLUMNS[OLD_HEAD].items():
        for table, column in columns:
            if column_values and desired[table][column] != values[kind]:
                raise ValueError("Original prompt cohort binding disagrees with column binding")
            desired[table][column] = values[kind]
    # The controller validates its pristine owned destination before alignment
    # and validates the final structure after it. Partial FK DDL is recreated.
    actual = source_column_collations(conn, schema, OLD_HEAD)
    changed = {
        (table, name)
        for table, columns in desired.items()
        for name, value in columns.items()
        if actual[table][name] != value
    }
    if not changed:
        return
    inspector = sa.inspect(conn)
    changed_fks = []
    quote = conn.dialect.identifier_preparer.quote_identifier
    for table in v0038.metadata.tables:
        for fk in inspector.get_foreign_keys(table, schema=schema):
            local = {(table, name) for name in fk["constrained_columns"]}
            remote = {(fk["referred_table"], name) for name in fk["referred_columns"]}
            if changed & (local | remote):
                conn.execute(
                    sa.text(
                        f"ALTER TABLE {qualified(schema, table)} DROP FOREIGN KEY "
                        f"{quote(str(fk['name']))}"
                    )
                )
                changed_fks.append((table, fk))
    for table, column in sorted(changed):
        definition = str(
            sa.schema.CreateColumn(v0038.metadata.tables[table].c[column]).compile(
                dialect=conn.dialect
            )
        )
        rendered = _render_column(definition, column, desired[table][column])
        conn.execute(sa.text(f"ALTER TABLE {qualified(schema, table)} MODIFY {rendered}"))
    for table, fk in changed_fks:
        fields = ",".join(identifier(c) for c in fk["constrained_columns"])
        references = ",".join(identifier(c) for c in fk["referred_columns"])
        actions = "".join(
            f" {clause} {fk['options'][option]}"
            for option, clause in (("ondelete", "ON DELETE"), ("onupdate", "ON UPDATE"))
            if option in fk.get("options", {})
        )
        conn.execute(
            sa.text(
                f"ALTER TABLE {qualified(schema, table)} ADD CONSTRAINT "
                f"{quote(str(fk['name']))} FOREIGN KEY ({fields}) REFERENCES "
                f"{qualified(schema, fk['referred_table'])} ({references}){actions}"
            )
        )


def schema_options(conn: Connection, source: str) -> tuple[str, str]:
    row = conn.execute(
        sa.text(
            "SELECT DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME "
            "FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:schema"
        ),
        {"schema": source},
    ).one()
    for value in row:
        identifier(value)
    return row[0], row[1]


def create_schema(conn: Connection, destination: str, source: str) -> None:
    charset, collation = schema_options(conn, source)
    conn.execute(
        sa.text(
            f"CREATE DATABASE {identifier(destination)} CHARACTER SET {charset} COLLATE {collation}"
        )
    )


def tables(conn: Connection, schema: str) -> set[str]:
    return set(
        conn.execute(
            sa.text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=:schema"),
            {"schema": schema},
        ).scalars()
    )


def head(conn: Connection, schema: str) -> str:
    values = (
        conn.execute(sa.text(f"SELECT version_num FROM {qualified(schema, 'alembic_version')}"))
        .scalars()
        .all()
    )
    if len(values) != 1 or values[0] not in (OLD_HEAD, INTERMEDIATE_HEAD, NEW_HEAD):
        raise ValueError("Unknown or multiple database heads")
    return values[0]
