"""Private snapshot backup, isolated restore and read-only maintenance capability checks.

The host owns writer fencing and deployment locks. This module never starts a runtime,
changes a source database, executes archived DDL, or prints database/row contents.
"""

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor import contracts, conversion, v0038
from qs_ai.maintenance.schema_refactor.layouts import (
    AUTO_INCREMENT_TABLES,
    NEW_HEAD,
    OLD_HEAD,
    identifier,
    qualified,
)
from qs_ai.maintenance.schema_refactor.layouts import (
    physical as physical_name,
)
from qs_ai.maintenance.schema_refactor.validation import require_schema

SOURCE_PRIVILEGES = {"SELECT", "INSERT", "CREATE", "ALTER", "DROP", "TRIGGER"}
TARGET_PRIVILEGES = SOURCE_PRIVILEGES | {
    "UPDATE",
    "DELETE",
    "INDEX",
    "REFERENCES",
    "EVENT",
    "EXECUTE",
}
STAGE_PRIVILEGES = {"SELECT", "INSERT", "DELETE", "CREATE", "ALTER", "DROP", "INDEX", "REFERENCES"}
ARCHIVE_PRIVILEGES = {"SELECT", "INSERT", "CREATE", "ALTER", "DROP"}


def _sha(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Invalid backup checksum identity")
    return value


def _contract_sha(head: str) -> str:
    filename = "v0038.py" if head == OLD_HEAD else "v0040.json"
    return hashlib.sha256(Path(__file__).with_name(filename).read_bytes()).hexdigest()


def _templates(conn: Connection | None, head: str) -> dict[str, dict[str, Any]]:
    if head not in (OLD_HEAD, NEW_HEAD):
        raise ValueError("Backup requires exact 0038 or 0040 storage")
    result = {}
    if head == OLD_HEAD:
        for table in v0038.metadata.sorted_tables:
            columns = {
                c.name: {
                    "type": str(c.type),
                    "collation": getattr(c.type, "collation", None),
                    "generated": False,
                }
                for c in table.columns
            }
            result[table.name] = {
                "columns": columns,
                "primary_key": [c.name for c in table.primary_key],
                "parents": {fk.column.table.name for c in table.columns for fk in c.foreign_keys},
                "create_sql": str(sa.schema.CreateTable(table).compile(dialect=conn.dialect))
                if conn is not None
                else None,
                "index_sql": [
                    str(sa.schema.CreateIndex(index).compile(dialect=conn.dialect))
                    for index in sorted(table.indexes, key=lambda i: str(i.name))
                ]
                if conn is not None
                else [],
            }
    else:
        for table in contracts.load()["tables"]:
            result[table["name"]] = {
                "columns": {
                    c["name"]: {
                        "type": c["type"],
                        "collation": c["collation"],
                        "generated": bool(c["computed"]),
                    }
                    for c in table["columns"]
                },
                "primary_key": table["primary_key"],
                "parents": {fk["table"] for fk in table["foreign_keys"]},
                "create_sql": table["create_sql"],
                "index_sql": table["index_sql"],
            }
    result["alembic_version"] = {
        "columns": {"version_num": {"type": "VARCHAR(32)", "collation": None, "generated": False}},
        "primary_key": ["version_num"],
        "parents": set(),
        "create_sql": "CREATE TABLE alembic_version (\nversion_num VARCHAR(32) NOT NULL,\n"
        "PRIMARY KEY(version_num)\n) ENGINE=InnoDB CHARSET=utf8mb4",
        "index_sql": [],
    }
    return result


def _character(kind: str) -> bool:
    return bool(re.match(r"(?:VARCHAR|CHAR|TINYTEXT|MEDIUMTEXT|LONGTEXT|TEXT)\b", kind, re.I))


def _validate_header(header: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if header.get("type") != "header" or header.get("format") != fmt.FORMAT:
        raise ValueError("Unknown backup format")
    identifier(header["source_schema"])
    if not re.fullmatch(r"[0-9a-f-]{36}", header["source_server_uuid"]):
        raise ValueError("Invalid source server identity")
    templates = _templates(None, header["head"])
    if header["contract_sha256"] != _contract_sha(header["head"]):
        raise ValueError("Backup versioned schema contract mismatch")
    for key in ("character_set", "collation"):
        identifier(header[key])
    if not re.fullmatch(r"[A-Z0-9_,]*", header["sql_mode"]):
        raise ValueError("Invalid captured session mode")
    if not isinstance(header["tables"], dict) or set(header["tables"]) != set(templates):
        raise ValueError("Backup table set differs from its frozen layout")
    physical_collations = {}
    for name, spec in templates.items():
        table = header["tables"][name]
        columns = sorted(c for c, value in spec["columns"].items() if not value["generated"])
        generated = sorted(c for c, value in spec["columns"].items() if value["generated"])
        if table["columns"] != columns or table["generated"] != generated:
            raise ValueError("Backup column shape differs from its frozen layout")
        chars = {c for c, value in spec["columns"].items() if _character(value["type"])}
        if set(table["collations"]) != chars or set(table["character_sets"]) != chars:
            raise ValueError("Backup character column binding is incomplete")
        for column in chars:
            collation, charset = table["collations"][column], table["character_sets"][column]
            identifier(collation)
            identifier(charset)
            if not collation.startswith(charset + "_"):
                raise ValueError("Backup character set binding mismatch")
            physical_collations[name, column] = collation
        identifier(table["table_collation"])
        identifier(table["table_character_set"])
        if not table["table_collation"].startswith(table["table_character_set"] + "_"):
            raise ValueError("Backup table character set binding mismatch")
        counter = table["auto_increment"]
        if counter is not None and (type(counter) is not int or not 1 <= counter <= 2**64 - 1):
            raise ValueError("Invalid captured auto increment floor")
        if not isinstance(table["create_sql"], str) or len(table["create_sql"]) > 1024 * 1024:
            raise ValueError("Invalid archived structural evidence")
        if table["ddl_sha256"] != hashlib.sha256(table["create_sql"].encode()).hexdigest():
            raise ValueError("Archived structural evidence digest mismatch")
    if header["head"] == OLD_HEAD:
        contracts.validate_column_collations(
            {name: header["tables"][name]["collations"] for name in v0038.metadata.tables}
        )
    else:
        prompt = {}
        for kind, fields in contracts.LEGACY_COLUMNS[NEW_HEAD].items():
            values = {physical_collations[pair] for pair in fields}
            if len(values) != 1:
                raise ValueError("Backup prompt cohort mismatch")
            prompt[kind] = values.pop()
        inherited = contracts.final_column_collations(None, prompt)
        for name, spec in templates.items():
            if name == "alembic_version":
                continue
            for column, value in spec["columns"].items():
                if not _character(value["type"]):
                    continue
                collation = physical_collations[name, column]
                expected = inherited.get((name, column), value["collation"])
                if expected and collation != expected:
                    raise ValueError("Backup explicit column collation mismatch")
                if not expected and not collation.startswith("utf8mb4_"):
                    raise ValueError("Backup inherited character set mismatch")
    return templates


def _capture(conn: Connection, schema: str, head: str) -> dict[str, Any]:
    specs = _templates(conn, head)
    columns = conn.execute(
        sa.text(
            "SELECT TABLE_NAME,COLUMN_NAME,CHARACTER_SET_NAME,COLLATION_NAME "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=:schema"
        ),
        {"schema": schema},
    ).all()
    chars = {(table, column): (charset, collation) for table, column, charset, collation in columns}
    rows = conn.execute(
        sa.text(
            "SELECT t.TABLE_NAME,t.TABLE_COLLATION,c.CHARACTER_SET_NAME,t.AUTO_INCREMENT "
            "FROM information_schema.TABLES t JOIN information_schema.COLLATIONS c "
            "ON c.COLLATION_NAME=t.TABLE_COLLATION WHERE t.TABLE_SCHEMA=:schema"
        ),
        {"schema": schema},
    ).all()
    tables = {}
    for name, collation, charset, counter in rows:
        spec = specs[name]
        ddl = conn.execute(sa.text(f"SHOW CREATE TABLE {qualified(schema, name)}")).one()[1]
        character_columns = {c for c, value in spec["columns"].items() if _character(value["type"])}
        tables[name] = {
            "columns": sorted(c for c, value in spec["columns"].items() if not value["generated"]),
            "generated": sorted(c for c, value in spec["columns"].items() if value["generated"]),
            "collations": {c: chars[name, c][1] for c in character_columns},
            "character_sets": {c: chars[name, c][0] for c in character_columns},
            "table_collation": collation,
            "table_character_set": charset,
            "auto_increment": int(counter) if counter is not None else None,
            "create_sql": ddl,
            "ddl_sha256": hashlib.sha256(ddl.encode()).hexdigest(),
        }
    header = {
        "type": "header",
        "format": fmt.FORMAT,
        "source_schema": schema,
        "source_server_uuid": str(conn.scalar(sa.text("SELECT @@server_uuid"))),
        "head": head,
        "contract_sha256": _contract_sha(head),
        "character_set": contracts.schema_options(conn, schema)[0],
        "collation": contracts.schema_options(conn, schema)[1],
        "sql_mode": str(conn.scalar(sa.text("SELECT @@session.sql_mode"))),
        "captured_at": datetime.now(UTC).isoformat(),
        "tables": tables,
    }
    _validate_header(header)
    return header


def _native_rows(
    conn: Connection, schema: str, name: str, table: dict[str, Any], spec: dict[str, Any]
) -> Iterator[list[bytes | None]]:
    fields = ",".join(
        f"CAST({identifier(c)} AS BINARY)" for c in table["columns"] + table["generated"]
    )
    ordering = ",".join(f"CAST({identifier(c)} AS BINARY)" for c in spec["primary_key"])
    result = conn.execute(
        sa.text(
            f"SELECT {fields} FROM {qualified(schema, name)} ORDER BY {ordering}"
        ).execution_options(stream_results=True)
    )
    try:
        for row in result:
            values = []
            for value in row:
                if value is not None and not isinstance(value, bytes):
                    raise ValueError("Driver did not return native binary column data")
                values.append(value)
            yield values
    finally:
        result.close()


def _logical_manifest(conn: Connection, schema: str, head: str) -> dict[str, Any]:
    # A recovery backup preserves even rows that conversion's domain precheck rejects.
    return {
        name: conversion.digest(conn, schema, name, head) for name in sorted(v0038.metadata.tables)
    }


def _identity(
    header: dict[str, Any], expected_server_uuid: str | None, expected_head: str | None
) -> None:
    if expected_server_uuid is not None and header["source_server_uuid"] != expected_server_uuid:
        raise ValueError("Backup belongs to another source server")
    if expected_head is not None and header["head"] != expected_head:
        raise ValueError("Backup source head mismatch")


def _summary(header: dict[str, Any], footer: dict[str, Any], checksum: str) -> dict[str, Any]:
    return {
        "format": fmt.FORMAT,
        "source_schema": header["source_schema"],
        "source_server_uuid": header["source_server_uuid"],
        "head": header["head"],
        "backup_sha256": checksum,
        "structure_sha256": fmt.digest(header),
        "manifest_sha256": fmt.digest(footer["manifest"]),
        "physical_manifest_sha256": fmt.digest(footer["physical_manifest"]),
        "tables": len(header["tables"]),
        "rows": sum(table["rows"] for table in footer["physical_manifest"].values()),
    }


def backup(
    conn: Connection,
    schema: str,
    path: Path,
    *,
    expected_server_uuid: str | None = None,
    expected_head: str | None = None,
) -> dict[str, Any]:
    """Use an otherwise idle, host-owned connection and one read-only RR snapshot."""
    identifier(schema)
    fmt.private_parent(path)
    if conn.in_transaction():
        raise ValueError("Backup requires an idle dedicated connection")
    if path.exists() or fmt.receipt_path(path).exists():
        raise ValueError("Backup output or receipt already exists")
    conn.execute(sa.text("SET SESSION time_zone='+00:00'"))
    conn.execute(sa.text("SET SESSION transaction_isolation='REPEATABLE-READ'"))
    conn.execute(sa.text("SET SESSION information_schema_stats_expiry=0"))
    conn.commit()
    conn.execute(sa.text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
    try:
        head = contracts.head(conn, schema)
        specs = _templates(conn, head)
        # Hold MDL for every known table before capturing DDL. DML stays snapshot-consistent.
        for name in sorted(specs):
            conn.execute(sa.text(f"SELECT 1 FROM {qualified(schema, name)} LIMIT 0")).close()
        require_schema(conn, head, schema, column_collations={})
        header = _capture(conn, schema, head)
        _identity(header, expected_server_uuid, expected_head)
        physical = {}
        with fmt.unpublished(path) as (_, raw):
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
                fmt.emit(stream, header)
                for name in _creation_order(specs):
                    table = header["tables"][name]
                    fmt.emit(stream, {"type": "table", "name": name})
                    checksum, count = hashlib.sha256(), 0
                    for values in _native_rows(conn, schema, name, table, specs[name]):
                        checksum.update(fmt.native_digest(values))
                        fmt.emit(stream, {"type": "row", "values": fmt.encode(values)})
                        count += 1
                    physical[name] = {"rows": count, "sha256": checksum.hexdigest()}
                    fmt.emit(stream, {"type": "end_table", "name": name, **physical[name]})
                footer: dict[str, Any] = {
                    "type": "footer",
                    "physical_manifest": physical,
                    "manifest": _logical_manifest(conn, schema, head),
                }
                for name in AUTO_INCREMENT_TABLES:
                    footer["manifest"][name]["auto_increment"] = header["tables"][
                        physical_name(name, head)
                    ]["auto_increment"]
                # Nontransactional DDL/default/collation/counter drift cannot silently pass.
                after = _capture(conn, schema, head)
                after["captured_at"] = header["captured_at"]
                if after != header:
                    raise ValueError("Source structural or auto increment evidence changed")
                fmt.emit(stream, footer)
            raw.flush()
            checksum_value = fmt.checksum(raw)
            size = os.fstat(raw.fileno()).st_size
    finally:
        conn.rollback()
    receipt = {**_summary(header, footer, checksum_value), "bytes": size}
    fmt.write_private(fmt.receipt_path(path), receipt)
    return {"backup": "passed", **receipt}


def _scan(stream: BinaryIO) -> tuple[dict[str, Any], dict[str, Any]]:
    reader = fmt.records(stream)
    header = next(reader)
    templates = _validate_header(header)
    physical = {}
    for name in _creation_order(templates):
        if next(reader) != {"type": "table", "name": name}:
            raise ValueError("Backup table order or completeness mismatch")
        checksum, count = hashlib.sha256(), 0
        columns = header["tables"][name]
        for record in reader:
            if record.get("type") == "end_table":
                expected = {
                    "type": "end_table",
                    "name": name,
                    "rows": count,
                    "sha256": checksum.hexdigest(),
                }
                if record != expected:
                    raise ValueError("Backup native table digest mismatch")
                physical[name] = {"rows": count, "sha256": checksum.hexdigest()}
                break
            if set(record) != {"type", "values"} or record["type"] != "row":
                raise ValueError("Unknown native backup record")
            values = fmt.decode(record["values"], len(columns["columns"] + columns["generated"]))
            checksum.update(fmt.native_digest(values))
            count += 1
        else:
            raise ValueError("Incomplete backup table")
    footer = next(reader)
    if set(footer) != {"type", "physical_manifest", "manifest"} or footer["type"] != "footer":
        raise ValueError("Incomplete backup footer")
    if footer["physical_manifest"] != physical or set(footer["manifest"]) != set(
        v0038.metadata.tables
    ):
        raise ValueError("Backup manifest table binding mismatch")
    if next(reader, None) is not None:
        raise ValueError("Unexpected trailing backup records")
    return header, footer


@contextmanager
def _verified(
    path: Path, expected_sha256: str | None = None
) -> Iterator[tuple[BinaryIO, dict[str, Any], dict[str, Any], dict[str, Any]]]:
    receipt = fmt.read_receipt(path)
    expected = (
        _sha(expected_sha256) if expected_sha256 is not None else _sha(receipt["backup_sha256"])
    )
    with fmt.private_file(path) as stream:
        actual = fmt.checksum(stream)
        if actual != expected or actual != receipt["backup_sha256"]:
            raise ValueError("Backup checksum differs from its trusted receipt")
        if os.fstat(stream.fileno()).st_size != receipt["bytes"]:
            raise ValueError("Backup size differs from its receipt")
        header, footer = _scan(stream)
        summary = _summary(header, footer, actual)
        if any(receipt.get(key) != value for key, value in summary.items()):
            raise ValueError("Backup source or manifest differs from its receipt")
        yield stream, header, footer, summary


def inspect_backup(
    schema: str,
    path: Path,
    *,
    expected_sha256: str | None = None,
    expected_server_uuid: str | None = None,
    expected_head: str | None = None,
) -> dict[str, Any]:
    with _verified(path, expected_sha256) as (_, header, _, summary):
        if header["source_schema"] != schema:
            raise ValueError("Backup source schema mismatch")
        _identity(header, expected_server_uuid, expected_head)
        return {"inspect": "passed", "verified": True, **summary}


def _creation_order(specs: dict[str, dict[str, Any]]) -> list[str]:
    result: list[str] = []
    pending = dict(specs)
    while pending:
        ready = sorted(name for name, spec in pending.items() if spec["parents"] <= set(result))
        if not ready:
            raise ValueError("Frozen schema foreign key graph is cyclic")
        for name in ready:
            result.append(name)
            del pending[name]
    return result


def _restore_ddl(
    conn: Connection, target: str, header: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    specs = _templates(conn, header["head"])
    # All identifiers and comparison scopes were checked before CREATE DATABASE.
    conn.execute(
        sa.text(
            f"CREATE DATABASE {identifier(target)} CHARACTER SET {header['character_set']} "
            f"COLLATE {header['collation']}"
        )
    )
    conn.execute(sa.text(f"USE {identifier(target)}"))
    for name in _creation_order(specs):
        spec, table = specs[name], header["tables"][name]
        sql = spec["create_sql"]
        # Archived SHOW CREATE text is evidence only. Every statement comes from frozen code.
        for column, collation in table["collations"].items():
            sql = contracts._render_column(sql, column, collation)
        body, closing, options = sql.rpartition(")")
        options, replacements = re.subn(
            r"\b(?:CHARSET|CHARACTER SET)=?\s*utf8mb4\b",
            "CHARSET=" + table["table_character_set"],
            options,
            flags=re.I,
        )
        if not closing or replacements != 1:
            raise ValueError("Frozen table character set declaration is unavailable")
        sql = (body + closing + options).rstrip() + " COLLATE " + table["table_collation"]
        conn.execute(sa.text(sql))
        for statement in spec["index_sql"]:
            conn.execute(sa.text(statement))
    return specs


def _insert_native(
    conn: Connection,
    target: str,
    name: str,
    table: dict[str, Any],
    spec: dict[str, Any],
    rows: list[list[bytes | None]],
) -> None:
    expressions = []
    for ordinal, column in enumerate(table["columns"]):
        value = f":v{ordinal}"
        kind = spec["columns"][column]["type"]
        if _character(kind):
            value = f"CONVERT({value} USING {table['character_sets'][column]})"
        elif kind == "JSON":
            value = f"CAST(CONVERT({value} USING utf8mb4) AS JSON)"
        elif not re.match(r"(?:TINYBLOB|MEDIUMBLOB|LONGBLOB|BLOB|VARBINARY|BINARY)\b", kind, re.I):
            value = f"CONVERT({value} USING ascii)"
        expressions.append(value)
    fields = ",".join(identifier(c) for c in table["columns"])
    conn.execute(
        sa.text(
            f"INSERT INTO {qualified(target, name)} ({fields}) VALUES ({','.join(expressions)})"
        ),
        [{f"v{i}": value for i, value in enumerate(row[: len(table["columns"])])} for row in rows],
    )


def restore(
    conn: Connection,
    schema: str,
    path: Path,
    target: str,
    expected_sha256: str,
    *,
    expected_server_uuid: str | None = None,
    expected_head: str | None = None,
) -> dict[str, Any]:
    identifier(target)
    if not target.startswith("ai_refactor_") or target == schema:
        raise ValueError("Restore requires a new isolated ai_refactor_ schema")
    if conn.in_transaction():
        raise ValueError("Restore requires an idle dedicated connection")
    with _verified(path, _sha(expected_sha256)) as (stream, header, footer, summary):
        if header["source_schema"] != schema:
            raise ValueError("Backup source schema mismatch")
        _identity(header, expected_server_uuid, expected_head)
        if conn.scalar(
            sa.text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:schema"),
            {"schema": target},
        ):
            raise ValueError("Restore destination already exists")
        server_uuid = str(conn.scalar(sa.text("SELECT @@server_uuid")))
        current = conn.scalar(sa.text("SELECT DATABASE()"))
        session_mode = str(conn.scalar(sa.text("SELECT @@session.sql_mode")))
        try:
            conn.execute(sa.text("SET SESSION time_zone='+00:00'"))
            conn.execute(sa.text("SET SESSION information_schema_stats_expiry=0"))
            conn.execute(sa.text("SET SESSION sql_mode=:mode"), {"mode": header["sql_mode"]})
            specs = _restore_ddl(conn, target, header)
            # One streaming pass in FK order; generated evidence is checked but never inserted.
            reader = fmt.records(stream)
            if next(reader) != header:
                raise ValueError("Backup header changed during restore")
            for name in _creation_order(specs):
                if next(reader) != {"type": "table", "name": name}:
                    raise ValueError("Backup table order changed during restore")
                batch: list[list[bytes | None]] = []
                for record in reader:
                    if record.get("type") == "row":
                        table = header["tables"][name]
                        batch.append(
                            fmt.decode(record["values"], len(table["columns"] + table["generated"]))
                        )
                        if len(batch) == 100:
                            _insert_native(conn, target, name, table, specs[name], batch)
                            batch = []
                    elif record.get("type") == "end_table" and record.get("name") == name:
                        if batch:
                            _insert_native(
                                conn, target, name, header["tables"][name], specs[name], batch
                            )
                        break
                    else:
                        raise ValueError("Backup records changed during restore")
                else:
                    raise ValueError("Incomplete backup during restore")
            if next(reader) != footer or next(reader, None) is not None:
                raise ValueError("Backup footer changed during restore")
            # Verify authenticated bytes again before publishing any restored row DML.
            if fmt.checksum(stream) != summary["backup_sha256"]:
                raise ValueError("Backup changed during restore")
            conn.commit()
            for name, table in header["tables"].items():
                if table["auto_increment"] is not None:
                    conn.execute(
                        sa.text(
                            f"ALTER TABLE {qualified(target, name)} "
                            f"AUTO_INCREMENT={table['auto_increment']}"
                        )
                    )
            require_schema(conn, header["head"], target, column_collations={})
            actual = _capture(conn, target, header["head"])
            for name, table in header["tables"].items():
                for key in (
                    "columns",
                    "generated",
                    "collations",
                    "character_sets",
                    "table_collation",
                    "table_character_set",
                    "auto_increment",
                ):
                    if actual["tables"][name][key] != table[key]:
                        raise ValueError("Restored physical metadata differs from the backup")
                checksum, count = hashlib.sha256(), 0
                for values in _native_rows(conn, target, name, table, specs[name]):
                    checksum.update(fmt.native_digest(values))
                    count += 1
                if footer["physical_manifest"][name] != {
                    "rows": count,
                    "sha256": checksum.hexdigest(),
                }:
                    raise ValueError("Restored native field bytes differ from the backup")
            if _logical_manifest(conn, target, header["head"]) != footer["manifest"]:
                raise ValueError("Restored logical facts differ from the backup")
            return {
                "restore": "passed",
                "verified": True,
                "target": target,
                "server_uuid": server_uuid,
                **summary,
            }
        except BaseException:
            conn.rollback()
            # Preserve the exact failed isolated target; never touch the source or other schemas.
            raise
        finally:
            conn.execute(sa.text("SET SESSION sql_mode=:mode"), {"mode": session_mode})
            if current:
                conn.execute(sa.text(f"USE {identifier(str(current))}"))


def _scope_matches(pattern: str, name: str, partial: bool) -> bool:
    expression, escaped = "", False
    for char in pattern:
        if escaped:
            expression += re.escape(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif not partial and char in "%_":
            expression += ".*" if char == "%" else "."
        else:
            expression += re.escape(char)
    return re.fullmatch(expression, name) is not None


def _privileges(
    grants: list[str],
    schema: str,
    partial: bool,
    *,
    prefix: bool = False,
    global_only: bool = False,
) -> set[str]:
    result: set[str] = set()
    denied: set[str] = set()
    for grant in grants:
        match = re.match(
            r"(GRANT|REVOKE) (.+?) ON (\*\.\*|`((?:``|[^`])+)`\.\*) (?:TO|FROM) ", grant, re.I
        )
        if match is None:
            continue
        scope = match[4].replace("``", "`") if match[4] is not None else None
        if global_only and scope is not None:
            continue
        matches = scope is None or _scope_matches(scope, schema, partial)
        if prefix and scope is not None:
            matches = (
                not partial
                and scope.endswith("%")
                and not scope.endswith("\\%")
                and _scope_matches(scope, schema, partial)
            )
        if not matches:
            continue
        privileges = {part.strip().upper() for part in match[2].split(",")}
        if "ALL PRIVILEGES" in privileges:
            privileges = TARGET_PRIVILEGES | ({"PROCESS"} if scope is None else set())
        (denied if match[1].upper() == "REVOKE" else result).update(privileges)
    return result - denied


def preflight(
    conn: Connection,
    schema: str,
    target: str | None = None,
    *,
    expected_server_uuid: str | None = None,
    expected_head: str | None = None,
    allow_external_incoming: bool = False,
) -> dict[str, Any]:
    """Report grants and effective metadata only; never test permission with CREATE."""
    identifier(schema)
    target = target or "ai_refactor_rehearsal_permission_probe"
    identifier(target)
    if not target.startswith("ai_refactor_"):
        raise ValueError("Preflight target must be isolated")
    head = contracts.head(conn, schema)
    _templates(None, head)
    source_uuid = str(conn.scalar(sa.text("SELECT @@server_uuid")))
    _identity(
        {"source_server_uuid": source_uuid, "head": head}, expected_server_uuid, expected_head
    )
    partial = bool(conn.scalar(sa.text("SELECT @@global.partial_revokes")))
    grants = [str(row[0]) for row in conn.execute(sa.text("SHOW GRANTS"))]
    source_privileges = _privileges(grants, schema, partial)
    global_privileges = _privileges(grants, "", partial, global_only=True)
    # A partial global SELECT revoke hides that schema's incoming FK children.
    full_metadata = "SELECT" in global_privileges and not any(
        re.match(r"REVOKE (?:.*\bSELECT\b|ALL PRIVILEGES).* ON ", grant, re.I) for grant in grants
    )
    suffix = target.removeprefix("ai_refactor_rehearsal_")
    if suffix == target:
        suffix = target.removeprefix("ai_refactor_")
    formal = {
        "target_create_restore": [
            target,
            "ai_refactor_" + suffix,
            "ai_refactor_probe_" + suffix,
        ],
        "archive_create": ["ai_backup_" + suffix, "ai_backup_probe_" + suffix],
        "rollback_create": ["ai_rollback_" + suffix, "ai_rollback_rehearsal_" + suffix],
        "failed_create": ["ai_failed_" + suffix, "ai_failed_rehearsal_" + suffix],
    }
    for names in formal.values():
        for name in names:
            identifier(name)
    capabilities = {
        "process": "PROCESS" in _privileges(grants, "", partial, global_only=True),
        "source_read": "SELECT" in source_privileges,
        "source_exchange": SOURCE_PRIVILEGES <= source_privileges,
        "source_trigger_visibility": "TRIGGER" in source_privileges,
        "source_routine_visibility": full_metadata
        or "SHOW_ROUTINE" in global_privileges
        or bool({"CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE"} & source_privileges),
        "source_event_visibility": "EVENT" in source_privileges,
        **{
            key: all(TARGET_PRIVILEGES <= _privileges(grants, name, partial) for name in names)
            for key, names in formal.items()
        },
        "random_stage_create": STAGE_PRIVILEGES
        <= _privileges(grants, "ai_refactor_stage_", partial, prefix=True),
        "random_old_create": ARCHIVE_PRIVILEGES
        <= _privileges(grants, "ai_refactor_old_", partial, prefix=True),
    }
    missing = [name for name, allowed in capabilities.items() if not allowed]
    require_schema(
        conn,
        head,
        schema,
        column_collations={},
        allow_external_incoming=allow_external_incoming,
    )
    objects = {
        table.lower(): int(
            conn.scalar(
                sa.text(f"SELECT COUNT(*) FROM information_schema.{table} WHERE {field}=:schema"),
                {"schema": schema},
            )
            or 0
        )
        for table, field in (("ROUTINES", "ROUTINE_SCHEMA"), ("EVENTS", "EVENT_SCHEMA"))
    }
    if any(objects.values()):
        missing.append("source_objects_absent")
    capabilities["source_objects_absent"] = not any(objects.values())
    existing = bool(
        conn.scalar(
            sa.text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:schema"),
            {"schema": target},
        )
    )
    if existing:
        missing.append("target_absent")
    capabilities["target_absent"] = not existing
    rows = conn.execute(
        sa.text(
            "SELECT TABLE_NAME,TABLE_ROWS,DATA_LENGTH,INDEX_LENGTH "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=:schema"
        ),
        {"schema": schema},
    ).all()
    return {
        "preflight": "passed" if not missing else "missing_capabilities",
        "ok": not missing,
        "source_schema": schema,
        "source_server_uuid": source_uuid,
        "head": head,
        "mysql_version": str(conn.scalar(sa.text("SELECT VERSION()"))),
        "partial_revokes": partial,
        "capabilities": capabilities,
        "missing_capabilities": missing,
        "source_schema_contract": "verified",
        "source_objects": objects,
        "cross_schema_fk_visibility": "all_schemas"
        if full_metadata
        else "schema_scoped_requires_admin_evidence",
        "tables": len(rows),
        "estimated_rows": sum(int(row[1] or 0) for row in rows),
        "allocated_bytes": sum(int((row[2] or 0) + (row[3] or 0)) for row in rows),
        "source_collation": contracts.schema_options(conn, schema)[1],
        "target": target,
        "target_exists": existing,
        "permission_check": "grant_metadata_only_no_database_created",
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("command", choices=("preflight", "backup", "restore", "inspect"))
    result.add_argument("--schema", required=True)
    result.add_argument("--path", type=Path)
    result.add_argument("--target")
    result.add_argument("--expected-sha256")
    result.add_argument("--expected-server-uuid")
    result.add_argument("--expected-head", choices=(OLD_HEAD, NEW_HEAD))
    return result


async def _command(args: argparse.Namespace) -> dict[str, Any]:
    options = {
        "expected_server_uuid": args.expected_server_uuid,
        "expected_head": args.expected_head,
    }
    if args.command != "preflight" and args.path is None:
        raise ValueError("Backup artifact path is required")
    if args.command == "inspect":
        return inspect_backup(
            args.schema, args.path, expected_sha256=args.expected_sha256, **options
        )
    url = os.environ.get("QS_AI_DATABASE_URL", "")
    if not url.startswith("mysql+asyncmy://") or any(c in url for c in "\r\n\x00"):
        raise ValueError("An environment-only maintenance connection is required")
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            if args.command == "preflight":
                return await connection.run_sync(
                    lambda c: preflight(c, args.schema, args.target, **options)
                )
            if args.command == "backup":
                return await connection.run_sync(
                    lambda c: backup(c, args.schema, args.path, **options)
                )
            if not args.target or not args.expected_sha256:
                raise ValueError("Restore requires an isolated target and trusted backup checksum")
            return await connection.run_sync(
                lambda c: restore(
                    c, args.schema, args.path, args.target, args.expected_sha256, **options
                )
            )
    finally:
        await engine.dispose()


def main() -> None:
    os.umask(0o077)
    args = parser().parse_args()
    try:
        result = asyncio.run(_command(args))
    except Exception as error:
        # Do not include exception strings, statement parameters, driver details or body bytes.
        print(json.dumps({args.command: "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))
    if result.get("ok") is False:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
