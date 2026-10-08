"""Private format3 evidence and a single guarded AI-only retirement transaction.

Connections are dedicated and host-owned. This package never starts/stops a runtime,
opens a borrowed transaction, restores into the source, or contacts QS/Mongo/providers.
"""

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.prompt_retirement import policy, snapshot
from qs_ai.maintenance.schema_refactor import backup as native
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, qualified

FORMAT = "qs-ai-prompt-retirement/v3"
RECEIPT_FORMAT = "qs-ai-prompt-retirement-restore/v3"
JOURNAL_FORMAT = "qs-ai-prompt-retirement-apply/v3"
SCOPE = {
    "asset_kind": "prompt",
    "owner_organization_id": 0,
    "template_id": policy.TEMPLATE_ID,
    "versions": list(policy.VERSIONS),
}


def execution(revision: str, image_id: str) -> dict[str, str]:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Execution revision must be a full immutable SHA")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise ValueError("Execution image must be a full immutable sha256 identity")
    # A caller cannot silently re-use an old plan with changed policy/controller code.
    files = (
        sorted(Path(__file__).parent.glob("*.py"))
        + sorted(Path(native.__file__).parent.glob("*.py"))
        + [Path(native.__file__).with_name("v0040.json")]
    )
    checksum = hashlib.sha256()
    for path in files:
        checksum.update(path.name.encode() + b"\x00" + path.read_bytes())
    return {"revision": revision, "image_id": image_id, "code_sha256": checksum.hexdigest()}


def read(path: Path, expected_format: str) -> dict[str, Any]:
    with fmt.private_file(path) as stream:
        raw = stream.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("Oversized retirement evidence")
    value = json.loads(raw, object_pairs_hook=fmt._pairs)
    if not isinstance(value, dict) or value.get("format") != expected_format:
        raise ValueError("Unknown retirement evidence; M5 and format3 cannot be interchanged")
    return value


def load_plan(path: Path, schema: str, revision: str, image_id: str) -> dict[str, Any]:
    value = read(path, FORMAT)
    unsigned = {key: item for key, item in value.items() if key != "plan_sha256"}
    if (
        value.get("plan_sha256") != fmt.digest(unsigned)
        or value.get("scope") != SCOPE
        or value.get("execution") != execution(revision, image_id)
        or value.get("inventory", {}).get("source", {}).get("source_schema") != schema
    ):
        raise ValueError("Retirement plan binding mismatch")
    if value["inventory"]["source"].get("head") != NEW_HEAD:
        raise ValueError("New Prompt retirement requires exact 0040; use historical M5 for 0038")
    return value


def _idle(conn: Connection) -> None:
    if conn.in_transaction():
        raise ValueError("Retirement requires an idle dedicated host-owned connection")


def _start(conn: Connection, *, readonly: bool) -> None:
    _idle(conn)
    conn.execute(sa.text("SET SESSION time_zone='+00:00'"))
    conn.execute(sa.text("SET SESSION transaction_isolation='REPEATABLE-READ'"))
    conn.execute(sa.text("SET SESSION information_schema_stats_expiry=0"))
    conn.commit()
    conn.execute(
        sa.text("START TRANSACTION" + (" WITH CONSISTENT SNAPSHOT, READ ONLY" if readonly else ""))
    )


def _capture(conn: Connection, schema: str) -> dict[str, Any]:
    _start(conn, readonly=True)
    try:
        return snapshot.capture(conn, schema)
    finally:
        conn.rollback()


def _binding(conn: Connection, value: dict[str, Any]) -> None:
    source = value["inventory"]["source"]
    if (
        str(conn.scalar(sa.text("SELECT @@server_uuid"))) != source["source_server_uuid"]
        or contracts.head(conn, source["source_schema"]) != source["head"]
    ):
        raise ValueError("Retirement source UUID or head mismatch")


def summary(value: dict[str, Any], operation: str, **extra: Any) -> dict[str, Any]:
    inventory = value["inventory"]
    source = inventory["source"]
    return {
        "prompt_retirement": "ok",
        "operation": operation,
        "status": "verified",
        "plan_sha256": value["plan_sha256"],
        "source_server_uuid": source["source_server_uuid"],
        "source_schema": source["source_schema"],
        "head": source["head"],
        "candidates": len(inventory["candidates"]),
        "retained": len(inventory["retained"]),
        "verified": True,
        **extra,
    }


def _classified(value: dict[str, Any], operation: str, state: str) -> dict[str, Any]:
    return summary(
        value,
        operation,
        status=state,
        verified=state != "unknown",
        prompt_retirement="failed" if state == "unknown" else "ok",
        application_repeated=False,
    )


def plan(
    conn: Connection, schema: str, path: Path, *, revision: str, image_id: str
) -> dict[str, Any]:
    bound = execution(revision, image_id)
    inventory = _capture(conn, schema)
    value = {
        "format": FORMAT,
        "created_at": datetime.now(UTC).isoformat(),
        "scope": SCOPE,
        "execution": bound,
        "inventory": inventory,
    }
    value["plan_sha256"] = fmt.digest(value)
    fmt.write_private(path, value)
    return summary(value, "plan")


def _same_inventory(conn: Connection, schema: str, value: dict[str, Any]) -> None:
    if _capture(conn, schema) != value["inventory"]:
        raise ValueError("Source facts, references, schema or counters changed; make a fresh plan")


def _backup_match(
    value: dict[str, Any], path: Path, expected_sha: str | None = None
) -> dict[str, Any]:
    source = value["inventory"]["source"]
    with native._verified(path, expected_sha) as (_, header, footer, result):
        if (
            snapshot.structure(header) != source
            or footer["physical_manifest"] != value["inventory"]["physical_manifest"]
            or footer["manifest"] != value["inventory"]["logical_manifest"]
        ):
            raise ValueError("Full backup differs from the accepted plan")
        return result


def restore(
    conn: Connection,
    schema: str,
    plan_path: Path,
    backup_path: Path,
    target: str,
    *,
    revision: str,
    image_id: str,
) -> dict[str, Any]:
    value = load_plan(plan_path, schema, revision, image_id)
    _idle(conn)
    _binding(conn, value)
    conn.rollback()
    evidence = _backup_match(value, backup_path)
    result = native.restore(
        conn,
        schema,
        backup_path,
        target,
        evidence["backup_sha256"],
        expected_server_uuid=evidence["source_server_uuid"],
        expected_head=evidence["head"],
    )
    conn.commit()
    return summary(
        value,
        "restore",
        restore_verified=True,
        restore_target=target,
        restore_target_retained=True,
        backup_sha256=evidence["backup_sha256"],
        physical_manifest_sha256=result["physical_manifest_sha256"],
    )


def backup(
    conn: Connection,
    schema: str,
    plan_path: Path,
    backup_path: Path,
    receipt_path: Path,
    target: str,
    *,
    revision: str,
    image_id: str,
) -> dict[str, Any]:
    value = load_plan(plan_path, schema, revision, image_id)
    fmt.private_parent(receipt_path)
    if receipt_path.exists():
        raise ValueError("Retirement restore receipt already exists")
    _same_inventory(conn, schema, value)
    source = value["inventory"]["source"]
    evidence = native.backup(
        conn,
        schema,
        backup_path,
        expected_server_uuid=source["source_server_uuid"],
        expected_head=source["head"],
    )
    _backup_match(value, backup_path, evidence["backup_sha256"])
    restored = restore(
        conn, schema, plan_path, backup_path, target, revision=revision, image_id=image_id
    )
    _same_inventory(conn, schema, value)
    # Only this invocation's newly created, fully verified clone may be cleaned up.
    _binding(conn, value)
    if contracts.head(conn, target) != source["head"]:
        raise ValueError("Restore proof target changed before cleanup")
    snapshot.require_schema(conn, source["head"], target, column_collations={})
    header = native._capture(conn, target, source["head"])
    full, _, _ = snapshot.physical(conn, target, source["head"], header, [])
    if full != value["inventory"]["physical_manifest"]:
        raise ValueError("Restore proof target data changed before cleanup")
    conn.execute(sa.text(f"DROP DATABASE {contracts.identifier(target)}"))
    conn.commit()
    receipt = {
        "format": RECEIPT_FORMAT,
        "plan_sha256": value["plan_sha256"],
        "execution": value["execution"],
        "source": source,
        "backup_sha256": evidence["backup_sha256"],
        "physical_manifest_sha256": evidence["physical_manifest_sha256"],
        "manifest_sha256": evidence["manifest_sha256"],
        "restore_verified": True,
        "restore_target": target,
        "restore_target_retained": False,
        "expires_at": (datetime.now(UTC) + timedelta(days=7)).isoformat(),
    }
    fmt.write_private(receipt_path, receipt)
    return summary(
        value,
        "backup",
        backup_sha256=evidence["backup_sha256"],
        manifest_sha256=evidence["manifest_sha256"],
        physical_manifest_sha256=evidence["physical_manifest_sha256"],
        restore_verified=restored["restore_verified"],
        restore_target=target,
        restore_target_retained=False,
    )


def check_receipt(value: dict[str, Any], backup_path: Path, receipt_path: Path) -> dict[str, Any]:
    receipt = read(receipt_path, RECEIPT_FORMAT)
    if (
        receipt.get("plan_sha256") != value["plan_sha256"]
        or receipt.get("execution") != value["execution"]
        or receipt.get("source") != value["inventory"]["source"]
        or receipt.get("restore_verified") is not True
        or datetime.fromisoformat(receipt["expires_at"]) <= datetime.now(UTC)
    ):
        raise ValueError("Independent restore receipt binding mismatch or expired")
    evidence = _backup_match(value, backup_path, receipt.get("backup_sha256"))
    if any(
        receipt.get(key) != evidence[key]
        for key in ("backup_sha256", "physical_manifest_sha256", "manifest_sha256")
    ):
        raise ValueError("Independent restore receipt manifest mismatch")
    return receipt


def verify(
    conn: Connection,
    schema: str,
    plan_path: Path,
    *,
    revision: str,
    image_id: str,
    backup_path: Path | None = None,
    receipt_path: Path | None = None,
    journal_path: Path | None = None,
) -> dict[str, Any]:
    value = load_plan(plan_path, schema, revision, image_id)
    if (backup_path is None) != (receipt_path is None):
        raise ValueError("Backup and restore receipt must be supplied together")
    if backup_path is not None and receipt_path is not None:
        check_receipt(value, backup_path, receipt_path)
    if journal_path is not None:
        journal = read(journal_path, JOURNAL_FORMAT)
        if (
            journal.get("plan_sha256") != value["plan_sha256"]
            or journal.get("source") != value["inventory"]["source"]
        ):
            raise ValueError("Application journal belongs to another plan")
        return _classified(value, "verify", classify(conn, value))
    _same_inventory(conn, schema, value)
    return summary(value, "verify", restore_verified=backup_path is not None)


def _save_journal(path: Path, value: dict[str, Any]) -> None:
    fmt.private_parent(path)
    if path.exists():
        existing = read(path, JOURNAL_FORMAT)
        if any(
            existing.get(key) != value.get(key) for key in ("plan_sha256", "source", "stopped_at")
        ):
            raise ValueError("Application journal immutable binding changed")
    descriptor, name = tempfile.mkstemp(prefix=".retirement-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(fmt.canonical(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fmt.fsync_parent(path)
    finally:
        temporary.unlink(missing_ok=True)


def classify(conn: Connection, value: dict[str, Any]) -> str:
    source, expected = value["inventory"]["source"], value["inventory"]
    _start(conn, readonly=True)
    try:
        _binding(conn, value)
        head, schema = source["head"], source["source_schema"]
        snapshot.require_schema(
            conn, head, schema, column_collations={}, allow_external_incoming=True
        )
        snapshot.visibility(conn, schema, head)
        target = "prompt_assets" if head == OLD_HEAD else "governance_asset_versions"
        if snapshot.incoming(conn, schema, target, head) != expected["foreign_keys"]:
            return "unknown"
        header = native._capture(conn, schema, head)
        full, protected, rows = snapshot.physical(
            conn, schema, head, header, expected["candidates"]
        )
        if snapshot.structure(header) != source or protected != expected["protected_manifest"]:
            return "unknown"
        if not rows:
            return "committed"
        if full == expected["physical_manifest"] and rows == {
            row["version"]: row["native_row_sha256"] for row in expected["candidates"]
        }:
            return "not_applied"
        return "unknown"
    finally:
        conn.rollback()


def _delete(conn: Connection, schema: str, head: str, row: dict[str, Any]) -> None:
    if head != NEW_HEAD:
        raise ValueError("New Prompt retirement deletion requires exact 0040")
    if (
        row["asset_kind"] != "prompt"
        or row["owner_organization_id"] != 0
        or not policy.candidate(row)
    ):
        raise ValueError("Delete is outside the fixed Prompt whitelist")
    if head == NEW_HEAD:
        table = "governance_asset_versions"
        predicate = (
            "asset_kind='prompt' AND owner_organization_id=0 AND asset_row_id=:row_id "
            "AND asset_id=:template_id AND version=:version AND fingerprint=:fingerprint "
            "AND package_sha256=:package_sha256 AND body_format='prompt_package_json' "
            "AND SHA2(body_bytes,256)=:body_sha256"
        )
    result = conn.execute(sa.text(f"DELETE FROM {qualified(schema, table)} WHERE {predicate}"), row)
    if result.rowcount != 1:
        raise ValueError("Guarded Prompt deletion did not match exactly one row")


def apply(
    conn: Connection,
    schema: str,
    plan_path: Path,
    backup_path: Path,
    receipt_path: Path,
    journal_path: Path,
    *,
    revision: str,
    image_id: str,
    writers_stopped: bool,
    stopped_at: str,
) -> dict[str, Any]:
    value = load_plan(plan_path, schema, revision, image_id)
    check_receipt(value, backup_path, receipt_path)
    _idle(conn)
    if journal_path.exists():
        previous = read(journal_path, JOURNAL_FORMAT)
        if (
            previous.get("plan_sha256") != value["plan_sha256"]
            or previous.get("source") != value["inventory"]["source"]
        ):
            raise ValueError("Application journal belongs to another plan")
        state = classify(conn, value)
        # An existing attempt is only classified, never repeated, including not_applied.
        return _classified(value, "apply", state)
    if not value["inventory"]["candidates"]:
        raise ValueError("Plan has no unreferenced Prompt candidates")
    control.deadline(stopped_at)
    _start(conn, readonly=False)
    journal = {
        "format": JOURNAL_FORMAT,
        "plan_sha256": value["plan_sha256"],
        "source": value["inventory"]["source"],
        "stopped_at": stopped_at,
        "phase": "pending",
    }
    try:
        control.fenced(conn, (schema,), writers_stopped)
        # Establish all source row/gap locks before opening the reference snapshot.
        for table in sorted(native._templates(conn, value["inventory"]["source"]["head"])):
            conn.execute(sa.text(f"SELECT 1 FROM {qualified(schema, table)} FOR UPDATE")).close()
        _binding(conn, value)
        current = snapshot.capture(conn, schema)
        if current != value["inventory"]:
            raise ValueError("Source drift before deletion; make a fresh plan")
        fmt.write_private(journal_path, journal)
        for row in current["candidates"]:
            control.deadline(stopped_at)
            snapshot.clock(conn)
            _delete(conn, schema, current["source"]["head"], row)
        full, protected, rows = snapshot.physical(
            conn, schema, current["source"]["head"], current["source"], current["candidates"]
        )
        if rows or protected != current["protected_manifest"] or full != protected:
            raise ValueError("Protected source facts changed during deletion")
        if (
            snapshot.structure(native._capture(conn, schema, current["source"]["head"]))
            != current["source"]
        ):
            raise ValueError("Source structure changed during deletion")
        control.fenced(conn, (schema,), writers_stopped)
        control.deadline(stopped_at)
        journal["phase"] = "commit_pending"
        _save_journal(journal_path, journal)
        conn.commit()
        journal["phase"] = "committed"
        _save_journal(journal_path, journal)
    except BaseException:
        # No source restore and no retry after an unknown COMMIT response. The private
        # pending journal lets a fresh owned connection classify all-present/all-absent.
        if not conn.invalidated:
            conn.rollback()
        raise
    if classify(conn, value) != "committed":
        raise ValueError("Post-commit source evidence is unknown; keep writers stopped")
    return summary(value, "apply", status="committed", application_repeated=False)
