"""Operator-driven, journalled isolated conversion and atomic table exchange."""

import hashlib
import json
import os
import re
import stat
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import Connection

from qs_ai.maintenance.schema_refactor import contracts
from qs_ai.maintenance.schema_refactor.conversion import align_counters, copy_data, manifest, verify
from qs_ai.maintenance.schema_refactor.layouts import (
    COPY_DEADLINE_SECONDS,
    INTERMEDIATE_HEAD,
    NEW_HEAD,
    OBSERVATION_SEEDS,
    OLD_HEAD,
    RETENTION_DAYS,
    identifier,
    physical,
    qualified,
)
from qs_ai.maintenance.schema_refactor.validation import require_schema


def save(path: Path, state: dict[str, Any]) -> None:
    """Never write DSNs/bodies. Atomic replace and owner-only file permissions."""
    payload = json.dumps(state, sort_keys=True, indent=2) + "\n"
    descriptor, temporary = tempfile.mkstemp(prefix=".schema-refactor-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode) or path.stat().st_mode & 0o077:
        raise ValueError("Journal must be a private regular file")
    state = json.loads(path.read_text())
    if state.get("format") != "qs-ai-schema-refactor/v1":
        raise ValueError("Unknown maintenance journal")
    return state


def _binding(conn: Connection, state: dict[str, Any]) -> None:
    if conn.scalar(sa.text("SELECT @@server_uuid")) != state["server_uuid"]:
        raise ValueError("Journal belongs to another database server")
    current_contract = hashlib.sha256(
        Path(__file__).with_name("v0040.json").read_bytes()
    ).hexdigest()
    if current_contract != state["contract_sha256"]:
        raise ValueError("Versioned conversion contract changed after planning")
    if not state.get("source_column_collations") or not state.get("legacy_prompt_collations"):
        raise ValueError("Journal is missing the original comparison cohort evidence")
    conn.info["source_column_collations"] = state["source_column_collations"]
    conn.info["legacy_prompt_collations"] = state["legacy_prompt_collations"]


def fenced(conn: Connection, schemas: tuple[str, ...], stopped: bool) -> None:
    if not stopped:
        raise ValueError("Stop all writers and hold the deployment locks before maintenance")
    grants = " ".join(str(r[0]).upper() for r in conn.execute(sa.text("SHOW GRANTS")))
    if "PROCESS" not in grants and "ALL PRIVILEGES ON *.*" not in grants:
        raise ValueError("Maintenance account needs PROCESS to verify writer fencing")
    for schema in schemas:
        count = conn.scalar(
            sa.text(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
                "WHERE DB=:schema AND ID<>CONNECTION_ID()"
            ),
            {"schema": schema},
        )
        if count:
            raise ValueError("Other database connections remain in a fenced schema")


def deadline(stopped_at: str) -> float:
    start = datetime.fromisoformat(stopped_at)
    if start.tzinfo is None or start.utcoffset() is None:
        raise ValueError("Stopped-at timestamp requires an explicit time zone")
    elapsed = (datetime.now(UTC) - start).total_seconds()
    if elapsed < 0 or elapsed >= COPY_DEADLINE_SECONDS:
        raise TimeoutError("Maintenance exchange deadline exceeded")
    return time.monotonic() + COPY_DEADLINE_SECONDS - elapsed


def plan(
    conn: Connection,
    source: str,
    target: str,
    archive: str,
    old_image: str,
    new_image: str,
    inherited: dict[str, Any] | None = None,
) -> dict[str, Any]:
    for schema in (source, target, archive):
        identifier(schema)
    if len({source, target, archive}) != 3:
        raise ValueError("Source, target and archive must be distinct")
    if not target.startswith(("ai_refactor_", "ai_rollback_")) or not archive.startswith(
        ("ai_backup_", "ai_failed_")
    ):
        raise ValueError("Targets and archives require isolated maintenance names")
    for image in (old_image, new_image):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
            raise ValueError("Images must be pinned to full sha256 identities")
    source_head = contracts.head(conn, source)
    if source_head == NEW_HEAD:
        if inherited is None:
            raise ValueError("Reverse plan requires the original forward journal")
        _binding(conn, inherited)
        if (
            inherited["source"] != source
            or inherited["source_head"] != OLD_HEAD
            or inherited["target_head"] != NEW_HEAD
            or inherited["phase"] not in ("switched", "cleaned")
        ):
            raise ValueError("Reverse plan must inherit this source's completed forward journal")
    elif inherited is not None:
        raise ValueError("Only a reverse plan may inherit a forward journal")
    columns = contracts.source_column_collations(conn, source, source_head)
    cohort = contracts.legacy_collations(conn, source, source_head)
    conn.info["source_column_collations"] = columns
    require_schema(conn, source_head, source)
    target_head = NEW_HEAD if source_head == OLD_HEAD else OLD_HEAD
    if source_head not in (OLD_HEAD, NEW_HEAD):
        raise ValueError("Intermediate storage cannot be a live source")
    existing = set(
        conn.execute(sa.text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")).scalars()
    )
    if target in existing or archive in existing:
        raise ValueError("Maintenance destinations already exist")
    return {
        "format": "qs-ai-schema-refactor/v1",
        "phase": "planned",
        "source": source,
        "target": target,
        "archive": archive,
        "source_head": source_head,
        "target_head": target_head,
        "old_image": old_image,
        "new_image": new_image,
        "server_uuid": conn.scalar(sa.text("SELECT @@server_uuid")),
        "created_at": datetime.now(UTC).isoformat(),
        "contract_sha256": hashlib.sha256(
            Path(__file__).with_name("v0040.json").read_bytes()
        ).hexdigest(),
        "source_column_collations": columns,
        "legacy_prompt_collations": cohort,
    }


def _persist(journal: Path | None, state: dict[str, Any]) -> None:
    if journal is not None:
        save(journal, state)


def _exists(conn: Connection, schema: str) -> bool:
    return bool(
        conn.scalar(
            sa.text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:schema"),
            {"schema": schema},
        )
    )


def _owned(state: dict[str, Any], schema: str) -> None:
    identifier(schema)
    if schema == state["source"] or schema not in state.get("owned_schemas", []):
        raise ValueError("Destination is not owned by this maintenance journal")
    if not schema.startswith(("ai_refactor_", "ai_rollback_", "ai_backup_", "ai_failed_")):
        raise ValueError("Destination is not an isolated maintenance schema")


def _pristine(conn: Connection, schema: str, head: str) -> bool:
    """Only empty owned tables and the exact Alembic observation seeds can be resumed."""
    observation = physical("ai_messaging_observations", head)
    for table in contracts.tables(conn, schema):
        if table in ("alembic_version", observation):
            continue
        if conn.scalar(sa.text(f"SELECT COUNT(*) FROM {qualified(schema, table)}")):
            return False
    rows = conn.execute(
        sa.text(
            f"SELECT kind,recorded_count,last_observed_at FROM {qualified(schema, observation)}"
        )
    ).all()
    return not rows or (
        len(rows) == len(OBSERVATION_SEEDS)
        and set(r[0] for r in rows) == set(OBSERVATION_SEEDS)
        and all(r[1] == 0 and r[2] is None for r in rows)
    )


def _empty_archive(conn: Connection, state: dict[str, Any], schema: str) -> None:
    _owned(state, schema)
    if _exists(conn, schema):
        if contracts.tables(conn, schema):
            raise ValueError("Owned archive contains unknown data or tables")
        if contracts.schema_options(conn, schema) != contracts.schema_options(
            conn, state["source"]
        ):
            raise ValueError("Owned archive has unexpected schema options")
    else:
        contracts.create_schema(conn, schema, state["source"])


def upgrade_isolated(conn: Connection, target: str, target_head: str, source: str) -> None:
    values = contracts.legacy_collations(conn, source, contracts.head(conn, source))
    if _exists(conn, target):
        existing_head = contracts.head(conn, target)
        require_schema(
            conn,
            existing_head,
            target,
            column_collations={} if existing_head == OLD_HEAD else None,
        )
        if not _pristine(conn, target, existing_head):
            raise ValueError("Owned destination contains pre-existing business data")
    else:
        contracts.create_schema(conn, target, source)
    current = conn.scalar(sa.text("SELECT DATABASE()"))
    previous = conn.info.get("legacy_prompt_collations")
    conn.info["legacy_prompt_collations"] = values
    conn.execute(sa.text(f"USE {identifier(target)}"))
    try:
        config = Config("alembic.ini")
        config.attributes["connection"] = conn
        command.upgrade(config, target_head)
        contracts.align_legacy_collations(
            conn,
            target,
            target_head,
            values,
            column_values=conn.info.get("source_column_collations"),
        )
    finally:
        if previous is None:
            conn.info.pop("legacy_prompt_collations", None)
        else:
            conn.info["legacy_prompt_collations"] = previous
        if current:
            conn.execute(sa.text(f"USE {identifier(current)}"))
    require_schema(conn, target_head, target)
    if contracts.legacy_collations(conn, target, target_head) != values:
        raise ValueError("Destination changed the inherited comparison cohort")


def _discard_pristine_owned(conn: Connection, state: dict[str, Any], target: str) -> None:
    """Recreate only this journal's empty, known DDL left by interrupted preparation."""
    _owned(state, target)
    known = {"alembic_version"} | {
        physical(table, head)
        for table in contracts.v0038.metadata.tables
        for head in (OLD_HEAD, INTERMEDIATE_HEAD, NEW_HEAD)
    }
    present = contracts.tables(conn, target)
    if not present <= known:
        raise ValueError("Owned destination contains unknown tables; recreation refused")
    if contracts.schema_options(conn, target) != contracts.schema_options(conn, state["source"]):
        raise ValueError("Owned destination has unexpected schema options")
    objects = conn.execute(
        sa.text(
            "SELECT ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=:schema"
        ),
        {"schema": target},
    ).all()
    if any(engine != "InnoDB" or kind != "BASE TABLE" for engine, kind in objects):
        raise ValueError("Owned destination contains unknown database objects")
    for catalog, column in (
        ("TRIGGERS", "TRIGGER_SCHEMA"),
        ("ROUTINES", "ROUTINE_SCHEMA"),
        ("EVENTS", "EVENT_SCHEMA"),
    ):
        if conn.scalar(
            sa.text(f"SELECT COUNT(*) FROM information_schema.{catalog} WHERE {column}=:schema"),
            {"schema": target},
        ):
            raise ValueError("Owned destination contains unknown database objects")
    observations = {
        physical("ai_messaging_observations", head)
        for head in (OLD_HEAD, INTERMEDIATE_HEAD, NEW_HEAD)
    }
    for table in present:
        if table == "alembic_version":
            versions = (
                conn.execute(sa.text(f"SELECT version_num FROM {qualified(target, table)}"))
                .scalars()
                .all()
            )
            revisions = {
                revision.revision
                for revision in ScriptDirectory.from_config(Config("alembic.ini")).walk_revisions()
            }
            if len(versions) > 1 or not set(versions) <= revisions:
                raise ValueError("Owned destination contains unknown migration evidence")
        elif table in observations:
            rows = conn.execute(
                sa.text(
                    f"SELECT kind,recorded_count,last_observed_at FROM {qualified(target, table)}"
                )
            ).all()
            if rows and (
                len(rows) != len(OBSERVATION_SEEDS)
                or set(r[0] for r in rows) != set(OBSERVATION_SEEDS)
                or any(r[1] != 0 or r[2] is not None for r in rows)
            ):
                raise ValueError("Owned destination contains pre-existing business data")
        elif conn.scalar(sa.text(f"SELECT COUNT(*) FROM {qualified(target, table)}")):
            raise ValueError("Owned destination contains pre-existing business data")
    external = conn.scalar(
        sa.text(
            "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE REFERENCED_TABLE_SCHEMA=:schema AND TABLE_SCHEMA<>:schema"
        ),
        {"schema": target},
    )
    if external:
        raise ValueError("External foreign key prevents recreation of owned destination")
    conn.execute(sa.text(f"DROP DATABASE {identifier(target)}"))


def _resume_isolated(
    conn: Connection,
    state: dict[str, Any],
    target: str,
    head: str,
    source: str,
) -> None:
    _owned(state, target)
    if _exists(conn, target):
        try:
            require_schema(conn, head, target, column_collations={} if head == OLD_HEAD else None)
        except ValueError:
            _discard_pristine_owned(conn, state, target)
        else:
            if not _pristine(conn, target, head):
                raise ValueError("Owned destination contains pre-existing business data")
    upgrade_isolated(conn, target, head, source)


def prepare(conn: Connection, state: dict[str, Any], journal: Path | None = None) -> None:
    _binding(conn, state)
    if state["phase"] not in ("planned", "prepare_pending"):
        raise ValueError("Prepare requires a planned journal")
    require_schema(conn, state["source_head"], state["source"])
    if state["phase"] == "planned":
        if any(_exists(conn, state[k]) for k in ("target", "archive")):
            raise ValueError("Maintenance destinations already exist")
        state["owned_schemas"] = [state["target"], state["archive"]]
        state["phase"] = "prepare_pending"
        _persist(journal, state)
    _owned(state, state["target"])
    _resume_isolated(conn, state, state["target"], state["target_head"], state["source"])
    _empty_archive(conn, state, state["archive"])
    state["phase"] = "prepared"


def _same_stop(state: dict[str, Any], key: str, supplied: str) -> str:
    original = state.get(key, supplied)
    if datetime.fromisoformat(original) != datetime.fromisoformat(supplied):
        raise ValueError("Retry must retain the original stopped-at deadline")
    return str(original)


def copy(
    conn: Connection,
    state: dict[str, Any],
    stopped: bool,
    stopped_at: str,
    journal: Path | None = None,
) -> None:
    _binding(conn, state)
    if state["phase"] not in ("prepared", "copy_pending", "copied"):
        raise ValueError("Copy requires an owned prepared target")
    stopped_at = _same_stop(state, "stopped_at", stopped_at)
    limit = deadline(stopped_at)
    fenced(conn, (state["source"], state["target"]), stopped)
    require_schema(conn, state["source_head"], state["source"])
    require_schema(conn, state["target_head"], state["target"])
    before = manifest(conn, state["source"], state["source_head"])
    if state["phase"] == "prepared":
        state.update(source_manifest=before, stopped_at=stopped_at, phase="copy_pending")
        _persist(journal, state)
    elif before != state["source_manifest"]:
        raise ValueError("Frozen source changed during interrupted copy")
    if not _pristine(conn, state["target"], state["target_head"]):
        verify(
            conn,
            state["source"],
            state["target"],
            state["source_head"],
            state["target_head"],
            counters=False,
        )
        align_counters(
            conn,
            state["source"],
            state["target"],
            state["source_head"],
            state["target_head"],
            deadline=limit,
        )
        verify(conn, state["source"], state["target"], state["source_head"], state["target_head"])
    else:
        copy_data(
            conn,
            state["source"],
            state["target"],
            state["source_head"],
            state["target_head"],
            deadline=limit,
        )
    deadline(stopped_at)
    state["phase"] = "copied"


def verified(conn: Connection, state: dict[str, Any], stopped: bool) -> None:
    _binding(conn, state)
    if state["phase"] not in ("copied", "verified", "switch_pending"):
        raise ValueError("Verify requires a completed copy")
    deadline(state["stopped_at"])
    fenced(conn, (state["source"], state["target"]), stopped)
    require_schema(conn, state["source_head"], state["source"])
    require_schema(conn, state["target_head"], state["target"])
    result = verify(
        conn, state["source"], state["target"], state["source_head"], state["target_head"]
    )
    if result != state["source_manifest"]:
        raise ValueError("Frozen source changed after copy")
    deadline(state["stopped_at"])
    state["phase"] = "verified"


def exchange(conn: Connection, source: str, target: str, archive: str) -> None:
    if contracts.tables(conn, archive):
        raise ValueError("Archive must be empty before exchange")
    old, new = sorted(contracts.tables(conn, source)), sorted(contracts.tables(conn, target))
    if not old or not new:
        raise ValueError("Exchange cannot use empty source/target")
    pairs = [f"{qualified(source, t)} TO {qualified(archive, t)}" for t in old]
    pairs += [f"{qualified(target, t)} TO {qualified(source, t)}" for t in new]
    conn.execute(sa.text("RENAME TABLE " + ",".join(pairs)))


def _exchange_before_deadline(
    conn: Connection,
    source: str,
    target: str,
    archive: str,
    stopped_at: str,
) -> None:
    remaining = deadline(stopped_at) - time.monotonic()
    previous = conn.scalar(sa.text("SELECT @@session.lock_wait_timeout"))
    conn.execute(
        sa.text("SET SESSION lock_wait_timeout=:seconds"), {"seconds": max(1, int(remaining))}
    )
    try:
        deadline(stopped_at)
        exchange(conn, source, target, archive)
    finally:
        if not conn.invalidated:
            conn.execute(sa.text("SET SESSION lock_wait_timeout=:seconds"), {"seconds": previous})


def _exchanged(conn: Connection, state: dict[str, Any]) -> bool:
    if "alembic_version" not in contracts.tables(conn, state["source"]):
        raise ValueError("Source has an unrecognized interrupted layout")
    head = contracts.head(conn, state["source"])
    if head == state["target_head"]:
        require_schema(conn, state["target_head"], state["source"])
        require_schema(conn, state["source_head"], state["archive"])
        if contracts.tables(conn, state["target"]):
            raise ValueError("Unknown exchange outcome: staging is not empty")
        return True
    if head != state["source_head"]:
        raise ValueError("Source has an unrecognized interrupted layout")
    return False


def _retained(
    state: dict[str, Any],
    schema: str,
    head: str,
    data: dict[str, Any],
    since: str,
) -> None:
    records = state.setdefault("retained_archives", [])
    if not any(r["schema"] == schema for r in records):
        records.append(dict(schema=schema, head=head, manifest=data, retained_since=since))


def switch(conn: Connection, state: dict[str, Any], stopped: bool, journal: Path) -> None:
    _binding(conn, state)
    if state["phase"] not in ("verified", "switch_pending", "switched"):
        raise ValueError("Switch requires verified data")
    fenced(conn, (state["source"], state["target"], state["archive"]), stopped)
    exchanged = _exchanged(conn, state)
    if state["phase"] == "switched":
        if not exchanged:
            raise ValueError("Completed exchange no longer matches the journal")
        return
    if not exchanged:
        verified(conn, state, stopped)
        state["phase"] = "switch_pending"
        save(journal, state)
        _exchange_before_deadline(
            conn, state["source"], state["target"], state["archive"], state["stopped_at"]
        )
    require_schema(conn, state["target_head"], state["source"])
    require_schema(conn, state["source_head"], state["archive"])
    if contracts.tables(conn, state["target"]):
        raise ValueError("Post-exchange staging is not empty")
    if any(
        manifest(conn, state[k], state[h]) != state["source_manifest"]
        for k, h in (("source", "target_head"), ("archive", "source_head"))
    ):
        raise ValueError("Post-exchange data differs; keep all writers stopped")
    state["phase"] = "switched"
    state["switched_at"] = state.get("switched_at", datetime.now(UTC).isoformat())
    state["runtime_may_have_written"] = True
    _retained(
        state,
        state["archive"],
        state["source_head"],
        state["source_manifest"],
        state["switched_at"],
    )
    save(journal, state)


def _rollback_complete(conn: Connection, state: dict[str, Any]) -> bool:
    head = contracts.head(conn, state["source"])
    if head == state["target_head"]:
        return False
    if head != state["source_head"]:
        raise ValueError("Unknown rollback exchange outcome")
    require_schema(conn, state["source_head"], state["source"])
    require_schema(conn, state["target_head"], state["rollback_archive"])
    if contracts.tables(conn, state["rollback_target"]):
        raise ValueError("Unknown rollback exchange outcome: staging is not empty")
    for schema, version in (
        (state["source"], state["source_head"]),
        (state["rollback_archive"], state["target_head"]),
    ):
        if manifest(conn, schema, version) != state["rollback_manifest"]:
            raise ValueError("Rollback post-exchange data mismatch; keep writers stopped")
    return True


def rollback(
    conn: Connection,
    state: dict[str, Any],
    stopped: bool,
    stopped_at: str,
    runtime_never_started: bool = False,
    journal: Path | None = None,
) -> None:
    _binding(conn, state)
    if state["phase"] not in ("switched", "switch_pending", "rollback_pending", "rolled_back"):
        raise ValueError("Rollback requires a completed exchange")
    source = state["source"]
    schemas = tuple(
        dict.fromkeys(
            (
                source,
                state["archive"],
                *[state[k] for k in ("rollback_target", "rollback_archive") if k in state],
            )
        )
    )
    fenced(conn, schemas, stopped)
    if state["phase"] == "rolled_back":
        require_schema(conn, state["source_head"], source)
        return
    if state["phase"] != "rollback_pending":
        deadline(stopped_at)
        if not _exchanged(conn, state):
            raise ValueError("Probe exchange outcome before attempting rollback")
        current = manifest(conn, source, state["target_head"])
        if runtime_never_started and (
            current != state["source_manifest"]
            or manifest(conn, state["archive"], state["source_head"]) != current
        ):
            raise ValueError("Fast rollback would discard new data")
        suffix = hashlib.sha256((state["source"] + state["created_at"]).encode()).hexdigest()[:12]
        destination = state["archive"] if runtime_never_started else "ai_rollback_" + suffix
        failed = "ai_failed_" + suffix
        if _exists(conn, failed) or (not runtime_never_started and _exists(conn, destination)):
            raise ValueError("Rollback destinations already exist")
        state.update(
            rollback_target=destination,
            rollback_archive=failed,
            rollback_manifest=current,
            rollback_stopped_at=stopped_at,
            rollback_mode="fast" if runtime_never_started else "reverse",
            phase="rollback_pending",
        )
        state.setdefault("owned_schemas", [state["target"], state["archive"]]).extend(
            s for s in (destination, failed) if s not in state["owned_schemas"]
        )
        state.setdefault("switched_at", datetime.now(UTC).isoformat())
        _retained(
            state,
            state["archive"],
            state["source_head"],
            state["source_manifest"],
            state["switched_at"],
        )
        _persist(journal, state)
    if not _rollback_complete(conn, state):
        stopped_at = _same_stop(state, "rollback_stopped_at", stopped_at)
        limit = deadline(stopped_at)
        if manifest(conn, source, state["target_head"]) != state["rollback_manifest"]:
            raise ValueError("Frozen active data changed during interrupted rollback")
        destination, failed = state["rollback_target"], state["rollback_archive"]
        _owned(state, destination)
        _empty_archive(conn, state, failed)
        if state["rollback_mode"] == "reverse":
            if not _exists(conn, destination):
                _resume_isolated(conn, state, destination, state["source_head"], source)
            else:
                try:
                    require_schema(conn, state["source_head"], destination)
                except ValueError:
                    _resume_isolated(conn, state, destination, state["source_head"], source)
            if _pristine(conn, destination, state["source_head"]):
                copy_data(
                    conn,
                    source,
                    destination,
                    state["target_head"],
                    state["source_head"],
                    deadline=limit,
                )
            verify(
                conn,
                source,
                destination,
                state["target_head"],
                state["source_head"],
                counters=False,
            )
            align_counters(
                conn,
                source,
                destination,
                state["target_head"],
                state["source_head"],
                deadline=limit,
            )
            result = verify(conn, source, destination, state["target_head"], state["source_head"])
        else:
            result = verify(conn, source, destination, state["target_head"], state["source_head"])
        if result != state["rollback_manifest"]:
            raise ValueError("Rollback source or destination changed")
        deadline(stopped_at)
        fenced(conn, (source, destination, failed), stopped)
        _exchange_before_deadline(conn, source, destination, failed, stopped_at)
        if not _rollback_complete(conn, state):
            raise ValueError("Rollback exchange did not complete")
    state["phase"] = "rolled_back"
    state["rolled_back_at"] = state.get("rolled_back_at", datetime.now(UTC).isoformat())
    if state["rollback_mode"] == "fast":
        for record in state["retained_archives"]:
            if record["schema"] == state["archive"]:
                record["empty_after_restore"] = True
    _retained(
        state,
        state["rollback_archive"],
        state["target_head"],
        state["rollback_manifest"],
        state["rolled_back_at"],
    )
    state["runtime_may_have_written"] = True
    _persist(journal, state)


def cleanup(conn: Connection, state: dict[str, Any], journal: Path | None = None) -> None:
    _binding(conn, state)
    if state["phase"] not in ("switched", "rolled_back", "cleanup_pending", "cleaned"):
        raise ValueError("Cleanup requires a completed cutover")
    records = state.get("retained_archives", [])
    if not records:
        raise ValueError("Cleanup requires recorded archive retention facts")
    # Validate every archive before dropping any; a newer rollback archive keeps its own 30 days.
    for record in records:
        archive = record["schema"]
        _owned(state, archive)
        if not archive.startswith(("ai_backup_", "ai_failed_")):
            raise ValueError("Cleanup only accepts recorded isolated archives")
        since = datetime.fromisoformat(record["retained_since"])
        if since.tzinfo is None or datetime.now(UTC) < since + timedelta(days=RETENTION_DAYS):
            raise ValueError("Archive has not reached its 30-day retention deadline")
        if not _exists(conn, archive):
            if not record.get("drop_pending") and not record.get("deleted_at"):
                raise ValueError("Recorded archive disappeared before cleanup")
            continue
        if record.get("empty_after_restore"):
            if contracts.tables(conn, archive):
                raise ValueError("Restored archive contains unknown data")
        else:
            require_schema(conn, record["head"], archive)
            if manifest(conn, archive, record["head"]) != record["manifest"]:
                raise ValueError("Archive changed since cutover; cleanup refused")
    state["phase"] = "cleanup_pending"
    _persist(journal, state)
    for record in records:
        archive = record["schema"]
        if _exists(conn, archive):
            record["drop_pending"] = True
            _persist(journal, state)
            conn.execute(sa.text(f"DROP DATABASE {identifier(archive)}"))
        record["deleted_at"] = record.get("deleted_at", datetime.now(UTC).isoformat())
        _persist(journal, state)
    state["phase"] = "cleaned"
    _persist(journal, state)
