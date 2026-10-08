"""Lossless server-side projections. No provider, broker, or application lifecycle."""

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.schema_refactor import v0038
from qs_ai.maintenance.schema_refactor.layouts import (
    ASSETS,
    AUTO_INCREMENT_TABLES,
    BATCH_SIZE,
    OBSERVATION_SEEDS,
    OLD_HEAD,
    identifier,
    physical,
    qualified,
)


@dataclass(frozen=True)
class Projection:
    columns: tuple[str, ...]
    expressions: tuple[str, ...]
    relation: str
    predicate: str = "1=1"

    def sql(self) -> str:
        fields = ",".join(
            f"{value} AS {identifier(name)}"
            for name, value in zip(self.columns, self.expressions, strict=True)
        )
        return f"SELECT {fields} FROM {self.relation} WHERE {self.predicate}"


def old_projection(schema: str, old: str, head: str, *, raw_evidence: bool = False) -> Projection:
    table = v0038.metadata.tables[old]
    columns = tuple(c.name for c in table.columns)
    relation = qualified(schema, physical(old, head)) + " s"
    values = {name: f"s.{identifier(name)}" for name in columns}
    predicate = "1=1"
    if head != OLD_HEAD:
        if old in ASSETS:
            kind, identity, version, body = ASSETS[old]
            predicate = (
                "s.asset_kind IN ('execution_policy','gate_policy')"
                if kind is None
                else f"s.asset_kind='{kind}'"
            )
            values[identity], values[version] = "s.asset_id", "s.version"
            values[body] = "s.body_bytes" if raw_evidence else "CONVERT(s.body_bytes USING utf8mb4)"
            if kind is None:
                values["kind"] = (
                    "CASE s.asset_kind WHEN 'execution_policy' THEN 'execution' ELSE 'gate' END"
                )
            if kind == "semantic_prompt":
                values["organization_id"] = "s.owner_organization_id"
        elif old in ("prompt_drafts", "semantic_draft_heads"):
            kind = "prompt" if old == "prompt_drafts" else "semantic"
            predicate = f"s.draft_kind='{kind}'"
        elif old in ("prompt_draft_revisions", "semantic_draft_versions"):
            kind = "prompt" if old == "prompt_draft_revisions" else "semantic"
            heads = qualified(schema, "governance_draft_heads")
            relation += f" JOIN {heads} h ON h.draft_row_id=s.draft_row_id"
            values["draft_id"] = "h.draft_id"
            values["snapshot_json"] = (
                "s.snapshot_bytes" if raw_evidence else "CONVERT(s.snapshot_bytes USING utf8mb4)"
            )
            if kind == "prompt":
                values["request_json"] = (
                    "s.request_bytes" if raw_evidence else "CONVERT(s.request_bytes USING utf8mb4)"
                )
            predicate = f"s.draft_kind='{kind}'"
        elif old == "external_requests":
            values = {"request_id": "s.request_id", "session_id": "s.id"}
            predicate = "s.request_id IS NOT NULL"
        elif old == "evaluation_run_policies":
            values["fingerprint"] = "s.frozen_execution_policy_fingerprint"
            values["definition_json"] = "s.frozen_execution_policy_json"
            predicate = "s.frozen_execution_policy_fingerprint IS NOT NULL"
        elif old in ("evaluation_generation_completions", "evaluation_semantic_completions"):
            kind = "generation" if old == "evaluation_generation_completions" else "semantic"
            predicate = f"s.kind='{kind}'"
    return Projection(columns, tuple(values[c] for c in columns), relation, predicate)


def _insert_batches(
    conn: Connection,
    destination: str,
    table: str,
    projection: Projection,
    order: tuple[str, ...],
    deadline: float | None,
) -> None:
    """Source is externally fenced; each batch is bounded, preserving native JSON."""
    offset = 0
    fields = ",".join(identifier(c) for c in projection.columns)
    ordering = ",".join(f"CAST({identifier(c)} AS BINARY)" for c in order)
    while True:
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("Migration copy deadline reached before exchange")
        statement = (
            f"INSERT INTO {qualified(destination, table)} ({fields}) "
            f"SELECT {fields} FROM ({projection.sql()}) projected "
            f"ORDER BY {ordering} LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        result = conn.execute(sa.text(statement).execution_options(stream_results=False))
        count = result.rowcount
        result.close()
        if count < BATCH_SIZE:
            break
        offset += count


def _forward_projection(source: str, old: str, target: str, target_head: str) -> Projection:
    original = old_projection(source, old, OLD_HEAD)
    columns, values = list(original.columns), list(original.expressions)
    relation = original.relation
    if old in ASSETS:
        kind, identity, version, body = ASSETS[old]
        kind_sql = (
            "CASE s.kind WHEN 'execution' THEN 'execution_policy' ELSE 'gate_policy' END"
            if kind is None
            else f"'{kind}'"
        )
        fmt = (
            "prompt_package_json"
            if kind == "prompt"
            else ("semantic_markdown" if kind == "semantic_prompt" else "definition_json")
        )
        columns = [
            "asset_kind",
            "owner_organization_id",
            "asset_id",
            "version",
            "fingerprint",
            "body_format",
            "body_bytes",
            "package_sha256",
            "source_ref",
            "imported_by",
            "created_at",
        ]
        values = [
            kind_sql,
            "s.organization_id" if kind == "semantic_prompt" else "0",
            f"s.{identifier(identity)}",
            f"s.{identifier(version)}",
            "s.fingerprint",
            f"'{fmt}'",
            f"CAST(s.{identifier(body)} AS BINARY)",
            "s.package_sha256" if kind == "prompt" else "NULL",
            "s.source_ref",
            "s.imported_by",
            "s.created_at",
        ]
    elif old in ("prompt_drafts", "semantic_draft_heads"):
        kind = "prompt" if old == "prompt_drafts" else "semantic"
        columns.insert(0, "draft_kind")
        values.insert(0, f"'{kind}'")
    elif old in ("prompt_draft_revisions", "semantic_draft_versions"):
        kind = "prompt" if old == "prompt_draft_revisions" else "semantic"
        heads = qualified(target, physical("prompt_drafts", target_head))
        relation += (
            f" JOIN {heads} h ON h.draft_kind='{kind}' AND h.organization_id=s.organization_id"
            " AND h.draft_id=s.draft_id"
        )
        columns = [
            "draft_row_id",
            "draft_kind",
            "organization_id",
            "revision",
            "snapshot_bytes",
            "snapshot_sha256",
            "command_id",
            "operator_user_id",
            "request_bytes",
        ]
        values = [
            "h.draft_row_id",
            f"'{kind}'",
            "s.organization_id",
            "s.revision",
            "CAST(s.snapshot_json AS BINARY)",
            "s.snapshot_sha256",
            "s.command_id" if kind == "prompt" else "NULL",
            "s.operator_user_id" if kind == "prompt" else "NULL",
            "CAST(s.request_json AS BINARY)" if kind == "prompt" else "NULL",
        ]
    elif old == "interpretation_sessions":
        relation += f" LEFT JOIN {qualified(source, 'external_requests')} e ON e.session_id=s.id"
        columns.append("request_id")
        values.append("e.request_id")
    elif old == "evaluation_runs":
        relation += (
            f" LEFT JOIN {qualified(source, 'evaluation_run_policies')} p ON p.run_id=s.run_id"
        )
        columns += ["frozen_execution_policy_fingerprint", "frozen_execution_policy_json"]
        values += ["p.fingerprint", "p.definition_json"]
    elif old in ("evaluation_generation_completions", "evaluation_semantic_completions"):
        kind = "generation" if old == "evaluation_generation_completions" else "semantic"
        columns.insert(0, "kind")
        values.insert(0, f"'{kind}'")
    return Projection(tuple(columns), tuple(values), relation)


def _order(projection: Projection, old: str) -> tuple[str, ...]:
    if old in ASSETS:
        return ("asset_kind", "owner_organization_id", "asset_id", "version")
    if old in ("prompt_draft_revisions", "semantic_draft_versions"):
        return ("draft_row_id", "revision")
    return tuple(
        c.name for c in v0038.metadata.tables[old].primary_key if c.name in projection.columns
    )


def _empty_target(conn: Connection, target: str, target_head: str) -> None:
    tables = {physical(t.name, target_head) for t in v0038.metadata.tables.values()}
    observation = physical("ai_messaging_observations", target_head)
    for table in tables:
        count = conn.scalar(sa.text(f"SELECT COUNT(*) FROM {qualified(target, table)}"))
        if count and table != observation:
            raise ValueError("Target contains unexpected pre-existing data")
    # Alembic seeds are not current facts. Replace only the exact known isolated seed set.
    rows = conn.execute(
        sa.text(
            f"SELECT kind,recorded_count,last_observed_at FROM {qualified(target, observation)}"
        )
    ).all()
    if rows and (
        set(r[0] for r in rows) != set(OBSERVATION_SEEDS)
        or any(r[1] != 0 or r[2] is not None for r in rows)
    ):
        raise ValueError("Target observation seed is not pristine")
    conn.execute(sa.text(f"DELETE FROM {qualified(target, observation)}"))


def precheck(conn: Connection, source: str, source_head: str) -> None:
    draft_tables: tuple[str, ...]
    if source_head == OLD_HEAD:
        pairs = [
            ("evaluation_run_policies", "run_id", "evaluation_runs", "run_id"),
            ("external_requests", "session_id", "interpretation_sessions", "id"),
        ]
        for child, column, parent, key in pairs:
            missing = conn.scalar(
                sa.text(
                    f"SELECT COUNT(*) FROM {qualified(source, child)} c "
                    f"LEFT JOIN {qualified(source, parent)} p "
                    f"ON c.{identifier(column)}=p.{identifier(key)} "
                    f"WHERE p.{identifier(key)} IS NULL"
                )
            )
            if missing:
                raise ValueError("Orphan source evidence prevents migration")
        draft_tables = ("prompt_drafts", "semantic_draft_heads")
        for table, column in (
            ("prompt_drafts", "organization_id"),
            ("prompt_draft_revisions", "operator_user_id"),
        ):
            if conn.scalar(
                sa.text(
                    f"SELECT COUNT(*) FROM {qualified(source, table)} "
                    f"WHERE {identifier(column)} < 0"
                )
            ):
                raise ValueError("Negative prompt scope/audit identity prevents migration")
    else:
        draft_tables = ("governance_draft_heads",)
    for table in draft_tables:
        invalid = conn.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM {qualified(source, table)} WHERE "
                "NOT REGEXP_LIKE(draft_id,"
                "'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$','c')"
            )
        )
        if invalid:
            raise ValueError("Noncanonical draft identity prevents migration")


def copy_data(
    conn: Connection,
    source: str,
    target: str,
    source_head: str,
    target_head: str,
    *,
    deadline: float | None = None,
) -> None:
    if source == target or source_head == target_head:
        raise ValueError("Conversion requires separate schemas and different layouts")
    precheck(conn, source, source_head)
    _empty_target(conn, target, target_head)
    forward = source_head == OLD_HEAD
    for table in v0038.metadata.sorted_tables:
        old = table.name
        if forward and old in ("external_requests", "evaluation_run_policies"):
            continue
        projection = (
            _forward_projection(source, old, target, target_head)
            if forward
            else old_projection(source, old, source_head)
        )
        order = _order(projection, old) if forward else tuple(c.name for c in table.primary_key)
        _insert_batches(conn, target, physical(old, target_head), projection, order, deadline)
    align_counters(conn, source, target, source_head, target_head, deadline=deadline)


def _counter(conn: Connection, schema: str, old: str, head: str) -> int | None:
    value = conn.scalar(
        sa.text(
            "SELECT AUTO_INCREMENT FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=:schema AND TABLE_NAME=:table"
        ),
        {"schema": schema, "table": physical(old, head)},
    )
    return int(value) if value is not None else None


def align_counters(
    conn: Connection,
    source: str,
    target: str,
    source_head: str,
    target_head: str,
    *,
    deadline: float | None = None,
) -> None:
    """Resume after ALTER's implicit commit; surrogate IDs deliberately have no contract."""
    for old in AUTO_INCREMENT_TABLES:
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("Migration counter deadline reached before exchange")
        value = _counter(conn, source, old, source_head)
        if value is None:
            raise ValueError("Source admission-lock auto-increment definition has drifted")
        if _counter(conn, target, old, target_head) != value:
            conn.execute(
                sa.text(
                    f"ALTER TABLE {qualified(target, physical(old, target_head))} "
                    f"AUTO_INCREMENT={value}"
                )
            )


def digest(conn: Connection, schema: str, old: str, head: str) -> dict[str, Any]:
    limit = conn.info.get("schema_refactor_deadline")

    def within_budget() -> None:
        if limit is not None and time.monotonic() >= limit:
            raise TimeoutError("Maintenance verification deadline reached; probe the layout")

    within_budget()
    # Hash consolidated BLOBs themselves; a lossy character conversion must never
    # make an invalid new body appear equivalent to a rewritten legacy TEXT value.
    projection = old_projection(schema, old, head, raw_evidence=True)
    fields = []
    for column in projection.columns:
        value = identifier(column)
        fields.append(
            f"CASE WHEN {value} IS NULL THEN 'SQL_NULL' "
            f"ELSE CONCAT(OCTET_LENGTH(CAST({value} AS BINARY)),':',"
            f"SHA2(CAST({value} AS BINARY),256)) END"
        )
    ordering = ",".join(
        f"CAST({identifier(c.name)} AS BINARY)" for c in v0038.metadata.tables[old].primary_key
    )
    statement = sa.text(
        f"SELECT {','.join(fields)} FROM ({projection.sql()}) p ORDER BY {ordering}"
    ).execution_options(stream_results=True)
    result = conn.execute(statement)
    sha, count = hashlib.sha256(), 0
    try:
        while True:
            within_budget()
            rows = result.fetchmany(BATCH_SIZE)
            within_budget()
            if not rows:
                break
            for row in rows:
                sha.update(json.dumps(tuple(row), separators=(",", ":")).encode())
                sha.update(b"\n")
                count += 1
                within_budget()
    finally:
        result.close()
    evidence: dict[str, Any] = {"rows": count, "sha256": sha.hexdigest()}
    if old in AUTO_INCREMENT_TABLES:
        evidence["auto_increment"] = _counter(conn, schema, old, head)
    return evidence


def manifest(conn: Connection, schema: str, head: str) -> dict[str, Any]:
    precheck(conn, schema, head)
    return {name: digest(conn, schema, name, head) for name in sorted(v0038.metadata.tables)}


def verify(
    conn: Connection,
    source: str,
    target: str,
    source_head: str,
    target_head: str,
    *,
    counters: bool = True,
) -> dict[str, Any]:
    before, after = manifest(conn, source, source_head), manifest(conn, target, target_head)
    compared_before, compared_after = before, after
    if not counters:
        compared_before, compared_after = (
            {
                name: {k: v for k, v in entry.items() if k != "auto_increment"}
                for name, entry in data.items()
            }
            for data in (before, after)
        )
    if compared_before != compared_after:
        changed = sorted(name for name in before if compared_before[name] != compared_after[name])
        raise ValueError("Data projection mismatch: " + ",".join(changed))
    return before
