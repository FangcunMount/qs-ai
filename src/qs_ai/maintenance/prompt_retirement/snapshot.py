"""Frozen-layout projections and native evidence; no application or broker lifecycle."""

import hashlib
import time
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.prompt_retirement import policy
from qs_ai.maintenance.schema_refactor import backup as native
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor import contracts, conversion, v0038
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, qualified
from qs_ai.maintenance.schema_refactor.validation import require_schema


def clock(conn: Connection) -> None:
    deadline = conn.info.get("schema_refactor_deadline")
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("Prompt retirement deadline exceeded")


def structure(header: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in header.items() if key != "captured_at"}


def visibility(conn: Connection, schema: str, head: str) -> dict[str, Any]:
    # Complete cross-schema visibility is required even when no FK is returned.
    report = native.preflight(conn, schema, expected_head=head, allow_external_incoming=True)
    required = (
        "source_read",
        "source_trigger_visibility",
        "source_routine_visibility",
        "source_event_visibility",
        "source_objects_absent",
    )
    if report["cross_schema_fk_visibility"] != "all_schemas" or any(
        not report["capabilities"][key] for key in required
    ):
        raise ValueError("Complete source object and cross-schema FK visibility is required")
    return {
        "cross_schema_fk_visibility": report["cross_schema_fk_visibility"],
        "source_objects": report["source_objects"],
    }


def incoming(conn: Connection, schema: str, table: str, head: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        sa.text(
            "SELECT k.TABLE_SCHEMA,k.TABLE_NAME,k.CONSTRAINT_NAME,k.COLUMN_NAME,"
            "k.REFERENCED_COLUMN_NAME,k.ORDINAL_POSITION,r.UPDATE_RULE,r.DELETE_RULE "
            "FROM information_schema.KEY_COLUMN_USAGE k "
            "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
            "ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME "
            "AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME "
            "WHERE k.REFERENCED_TABLE_SCHEMA=:schema AND k.REFERENCED_TABLE_NAME=:table "
            "ORDER BY k.TABLE_SCHEMA,k.TABLE_NAME,k.CONSTRAINT_NAME,k.ORDINAL_POSITION"
        ),
        {"schema": schema, "table": table},
    ).all()
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for child_schema, child, name, column, parent, _, update, delete in rows:
        item = groups.setdefault(
            (child_schema, child, name),
            {
                "schema": child_schema,
                "table": child,
                "constraint": name,
                "columns": [],
                "parent_columns": [],
                "on_update": update,
                "on_delete": delete,
            },
        )
        item["columns"].append(column)
        item["parent_columns"].append(parent)
    for item in groups.values():
        # This is the only frozen incoming FK that cannot reference a Prompt row.
        # require_schema independently verifies its exact current definition.
        item["prompt_reference"] = not (
            head == NEW_HEAD
            and item["schema"] == schema
            and item["table"] == "governance_profile_registrations"
            and item["columns"] == ["profile_id", "profile_version"]
            and item["parent_columns"] == ["profile_id_key", "version"]
            and item["on_update"] in ("RESTRICT", "NO ACTION")
            and item["on_delete"] in ("RESTRICT", "NO ACTION")
        )
    return list(groups.values())


def candidates(conn: Connection, schema: str, head: str) -> list[dict[str, Any]]:
    if head != NEW_HEAD:
        raise ValueError("New Prompt retirement candidates require exact 0040")
    projection = conversion.old_projection(schema, "prompt_assets", head, raw_evidence=True)
    fields = projection.sql()
    if head == NEW_HEAD:
        fields = fields.replace("SELECT ", "SELECT s.asset_row_id AS _row_id,", 1)
        fields += " AND s.owner_organization_id=0"
    else:
        fields = fields.replace("SELECT ", "SELECT NULL AS _row_id,", 1)
    result = []
    for raw in conn.execute(sa.text(fields)).mappings():
        row = dict(raw)
        if not policy.candidate(row):
            continue
        body = row["package_json"]
        if isinstance(body, str):
            body = body.encode("utf-8")
        if not isinstance(body, bytes):
            raise ValueError("Prompt body is not native text bytes")
        result.append(
            {
                "template_id": row["template_id"],
                "version": row["version"],
                "asset_kind": "prompt",
                "owner_organization_id": 0,
                "row_id": row["_row_id"],
                "fingerprint": row["fingerprint"],
                "package_sha256": row["package_sha256"],
                "body_sha256": hashlib.sha256(body).hexdigest(),
            }
        )
    if len(result) > len(policy.VERSIONS):
        raise ValueError("Prompt whitelist is not unique")
    return sorted(result, key=lambda row: row["version"])


def audit(conn: Connection, schema: str, head: str, assets: list[dict[str, Any]]) -> None:
    for asset in assets:
        asset["references"] = []
    # Every original logical column is projected from the frozen current layout.
    # This covers all seven asset kinds, both draft kinds and both completion kinds;
    # generated keys are derived evidence, not additional unversioned identities.
    for name in sorted(v0038.metadata.tables):
        clock(conn)
        projection = conversion.old_projection(schema, name, head, raw_evidence=True)
        sql = projection.sql()
        if head == NEW_HEAD and name == "prompt_assets":
            sql = sql.replace("SELECT ", "SELECT s.asset_row_id AS _row_id,", 1)
            sql += " AND s.owner_organization_id=0"
        result = conn.execute(sa.text(sql).execution_options(stream_results=True))
        try:
            for raw in result.mappings():
                clock(conn)
                row = dict(raw)
                for asset in assets:
                    if (
                        name == "prompt_assets"
                        and (row.get("template_id"), row.get("version"))
                        == (asset["template_id"], asset["version"])
                        and (head == OLD_HEAD or row.get("_row_id") == asset["row_id"])
                    ):
                        # Exclude exactly this physical Prompt row; other selected
                        # candidates and all other asset kinds still retain references.
                        continue
                    if name not in asset["references"] and policy.references(row, asset):
                        asset["references"].append(name)
        finally:
            result.close()


def physical(
    conn: Connection,
    schema: str,
    head: str,
    header: dict[str, Any],
    selected: list[dict[str, Any]],
    *,
    lock: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    specs = native._templates(conn, head)
    all_rows, protected, selected_rows = {}, {}, {}
    target = "prompt_assets" if head == OLD_HEAD else "governance_asset_versions"
    keys = {(row["template_id"], row["version"]): row for row in selected}
    for name in sorted(specs):
        clock(conn)
        if lock:
            # Hold MDL and row/gap locks across the entire reference universe, including
            # currently empty tables, until the single source transaction settles.
            locked = conn.execute(sa.text(f"SELECT 1 FROM {qualified(schema, name)} FOR UPDATE"))
            locked.close()
        full, keep, total, remaining = hashlib.sha256(), hashlib.sha256(), 0, 0
        table = header["tables"][name]
        columns = table["columns"] + table["generated"]
        for values in native._native_rows(conn, schema, name, table, specs[name]):
            clock(conn)
            raw = dict(zip(columns, values, strict=True))
            record = fmt.native_digest(values)
            full.update(record)
            total += 1
            key = None
            if name == target:
                if head == OLD_HEAD:
                    key = (raw["template_id"].decode(), raw["version"].decode())
                elif raw["asset_kind"] == b"prompt" and raw["owner_organization_id"] == b"0":
                    key = (raw["asset_id"].decode(), raw["version"].decode())
            if key in keys:
                asset = keys[key]
                if head == NEW_HEAD and raw["asset_row_id"] != str(asset["row_id"]).encode():
                    raise ValueError("Prompt row identity changed")
                if head == NEW_HEAD and raw["profile_id_key"] is not None:
                    raise ValueError("Prompt generated Profile key must be SQL NULL")
                selected_rows[asset["version"]] = hashlib.sha256(record).hexdigest()
            else:
                keep.update(record)
                remaining += 1
        all_rows[name] = {"rows": total, "sha256": full.hexdigest()}
        protected[name] = {"rows": remaining, "sha256": keep.hexdigest()}
    return all_rows, protected, selected_rows


def capture(conn: Connection, schema: str, *, lock: bool = False) -> dict[str, Any]:
    head = contracts.head(conn, schema)
    if head != NEW_HEAD:
        raise ValueError("New Prompt retirement requires exact 0040; use historical M5 for 0038")
    require_schema(conn, head, schema, column_collations={}, allow_external_incoming=True)
    visible = visibility(conn, schema, head)
    header = native._capture(conn, schema, head)
    target = "prompt_assets" if head == OLD_HEAD else "governance_asset_versions"
    foreign_keys = incoming(conn, schema, target, head)
    assets = candidates(conn, schema, head)
    audit(conn, schema, head, assets)
    blocked = any(row["prompt_reference"] for row in foreign_keys)
    selected = [
        row
        for row in assets
        if row["fingerprint"] and row["package_sha256"] and not row["references"] and not blocked
    ]
    full, protected, selected_rows = physical(conn, schema, head, header, selected, lock=lock)
    for row in selected:
        row["native_row_sha256"] = selected_rows[row["version"]]
    after = native._capture(conn, schema, head)
    if structure(after) != structure(header):
        raise ValueError("Source structure or counters changed during inventory")
    return {
        "source": structure(header),
        "visibility": visible,
        "foreign_keys": foreign_keys,
        "candidates": selected,
        "retained": [row for row in assets if row not in selected],
        "physical_manifest": full,
        "protected_manifest": protected,
        "logical_manifest": native._logical_manifest(conn, schema, head),
    }
