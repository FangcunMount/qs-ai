"""Immutable plan, private backup, real restore verification, guarded application."""

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts.retirement.policy import COLLECTIONS, EVENTS, SHARED_TABLES, TECHNICAL_TABLES
from scripts.retirement.prompt_policy import TEMPLATE_ID, VERSIONS


class Stop(RuntimeError):
    """A safety condition failed; messages must not include database content."""


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def write(path: Path, value) -> None:
    if path.is_symlink():
        raise Stop("symlink artifact refused")
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(canonical(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read(path: Path):
    if path.is_symlink() or path.stat().st_mode & 0o077:
        raise Stop("artifact permissions must exclude group and other users")
    return json.loads(path.read_bytes())


def summary(snapshot):
    return {
        key: {
            **{name: value for name, value in item.items() if name != "rows"},
            "selected_ids": [
                row["id"] for row in item["rows"] if isinstance(row, dict) and "id" in row
            ],
        }
        for key, item in snapshot.items()
    }


def inventory(adapters):
    return {adapter.name: summary(adapter.snapshot()) for adapter in adapters}


def plan(directory: Path, adapters):
    if directory.exists():
        raise Stop("use a new backup directory for each plan")
    if any((parent / ".git").exists() for parent in (directory, *directory.parents)):
        raise Stop("backups must be outside a Git worktree")
    directory.mkdir(mode=0o700, parents=True)
    document = {
        "version": 1,
        "scope": {
            "mongo_collections": COLLECTIONS,
            "events": EVENTS,
            "sql_message_tables": SHARED_TABLES,
            "sql_inventory_only": TECHNICAL_TABLES,
            "retired_prompts": {"template_id": TEMPLATE_ID, "versions": VERSIONS},
        },
        "created_at": datetime.now(UTC).isoformat(),
        "targets": {a.name: a.identity for a in adapters},
        "objects": inventory(adapters),
    }
    write(directory / "plan.json", document)
    return digest(document)


def load_plan(directory: Path, adapters, expected: str):
    if directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise Stop("backup directory must be private")
    document = read(directory / "plan.json")
    if digest(document) != expected or document.get("version") != 1:
        raise Stop("plan digest or version mismatch")
    if document["targets"] != {a.name: a.identity for a in adapters}:
        raise Stop("database targets changed")
    return document


def backup(directory: Path, adapters, expected: str):
    document = load_plan(directory, adapters, expected)
    if (directory / "backup.json").exists():
        raise Stop("backup already exists; do not overwrite it")
    snapshots = {a.name: a.snapshot() for a in adapters}
    if {k: summary(v) for k, v in snapshots.items()} != document["objects"]:
        raise Stop("objects, content, schema or references changed since planning")
    archive = {"plan_sha256": expected, "snapshots": snapshots}
    write(directory / "backup.json", archive)
    # Read from disk, not in-memory rows, then rebuild selected objects in isolated databases.
    saved = read(directory / "backup.json")
    if digest(saved) != digest(archive):
        raise Stop("backup readback mismatch")
    for adapter in adapters:
        adapter.verify_restore(saved["snapshots"][adapter.name])
    if inventory(adapters) != document["objects"]:
        raise Stop("database changed during backup and restoration verification")
    receipt = {
        "plan_sha256": expected,
        "backup_sha256": digest(saved),
        "verified_at": datetime.now(UTC).isoformat(),
        "expires_at": (datetime.now(UTC) + timedelta(days=7)).isoformat(),
    }
    write(directory / "restore-verification.json", receipt)
    return receipt


def checked_backup(directory, adapters, expected, *, require_current=True):
    document = load_plan(directory, adapters, expected)
    receipt = read(directory / "restore-verification.json")
    archive = read(directory / "backup.json")
    if (
        receipt["plan_sha256"] != expected
        or archive["plan_sha256"] != expected
        or receipt["backup_sha256"] != digest(archive)
    ):
        raise Stop("backup or restoration receipt mismatch")
    if require_current and datetime.fromisoformat(receipt["expires_at"]) <= datetime.now(UTC):
        raise Stop("backup retention window expired; prepare a new verified backup")
    if {k: summary(v) for k, v in archive["snapshots"].items()} != document["objects"]:
        raise Stop("backup does not match the reviewed inventory")
    return document, archive


def apply(directory: Path, adapters, expected: str, evidence: dict):
    document, archive = checked_backup(directory, adapters, expected)
    for key in ("acceptance_verified", "writers_stopped", "old_events_drained"):
        if evidence.get(key) is not True:
            raise Stop("production acceptance, writer stop and directed drain evidence required")
    for key in ("qs_sha", "ai_sha", "evidence_reference"):
        if not isinstance(evidence.get(key), str) or not evidence[key].strip():
            raise Stop("release and acceptance evidence references required")
    if (directory / "application.json").exists():
        raise Stop("application was already attempted; inspect the journal and restore if needed")
    if inventory(adapters) != document["objects"]:
        raise Stop("objects, content, schema or references changed before deletion")
    retention = read(directory / "restore-verification.json")
    retention["expires_at"] = (datetime.now(UTC) + timedelta(days=7)).isoformat()
    write(directory / "restore-verification.json", retention)
    journal = {"plan_sha256": expected, "evidence": evidence, "completed": [], "status": "started"}
    write(directory / "application.json", journal)
    for adapter in adapters:
        # Recheck immediately before each store. Shared services must remain quiescent.
        if summary(adapter.snapshot()) != document["objects"][adapter.name]:
            raise Stop("database changed between preflight and application; inspect journal")
        adapter.apply(archive["snapshots"][adapter.name])
        adapter.verify_deleted(archive["snapshots"][adapter.name])
        journal["completed"].append(adapter.name)
        write(directory / "application.json", journal)
    retention["expires_at"] = (datetime.now(UTC) + timedelta(days=7)).isoformat()
    write(directory / "restore-verification.json", retention)
    journal["status"] = "completed"
    journal["completed_at"] = datetime.now(UTC).isoformat()
    write(directory / "application.json", journal)
    return journal
