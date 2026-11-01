"""Bounded read-only fingerprints and private, durable review material.

Fingerprints are comparison evidence, not a backup of retained business data.
No messaging SDK, application lifecycle, model client or recovery loop starts here.
"""

import hashlib
import json
import os
import re
import stat
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

HEAD = "0044_empty_publisher"
PREVIOUS = "0043_native_transaction_limit"
TARGET = "messaging_outbox"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            raise ValueError("Duplicate review field")
        result[key] = value
    return result


def private_bytes(path: str, expected: str, limit: int = 4_194_304) -> bytes:
    if not isinstance(path, str) or not Path(path).is_absolute() or not HEX64.fullmatch(expected):
        raise ValueError("Absolute private material and exact SHA256 required")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
        ):
            raise ValueError("Owned private0600 material required")
        raw = stream.read(limit + 1)
    if not raw or len(raw) > limit or digest(raw) != expected:
        raise ValueError("Reviewed material changed")
    return raw


def document(path: str, expected: str) -> dict[str, Any]:
    result = json.loads(private_bytes(path, expected), object_pairs_hook=pairs)
    if not isinstance(result, dict):
        raise ValueError("Review object required")
    return result


def write_private(path: str, raw: bytes) -> None:
    """Create once under an owned0700 directory; pin its inode through fsync."""
    target = Path(path)
    if not target.is_absolute():
        raise ValueError("Absolute private path required")
    parent = os.open(str(target.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(parent)
        if stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.geteuid():
            raise ValueError("Owned private0700 directory required")
        fd = os.open(
            target.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent,
        )
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(parent)
    finally:
        os.close(parent)


def current_receipt(value: dict[str, Any]) -> None:
    now = datetime.now(UTC)
    issued = datetime.fromisoformat(value["recorded_at"])
    expiry = datetime.fromisoformat(value["expires_at"])
    if (
        issued.tzinfo is None
        or expiry.tzinfo is None
        or not now - timedelta(hours=1) <= issued <= now
        or not now < expiry <= issued + timedelta(hours=1)
    ):
        raise ValueError("Current bounded window receipt required")


def writer_receipt(
    value: dict[str, Any],
    *,
    source: str,
    operation: str,
    identity: str,
    current: bool = True,
) -> None:
    """Validate owner attestation; this cannot discover arbitrary external writers."""
    if (
        value.get("schema") != "ai-publisher-writer-exclusion/v1"
        or value.get("source_sha") != source
        or value.get("operation_id") != operation
        or value.get("mysql_identity_sha256") != identity
        or value.get("writers_excluded") is not True
        or value.get("all_writer_scopes_reviewed") is not True
        or value.get("broker_pending_preserved") is not True
        or value.get("business_reopened") is not False
        or type(value.get("running_writers")) is not int
        or value["running_writers"] != 0
        or type(value.get("unfinished_work")) is not int
        or value["unfinished_work"] != 0
        or value.get("data_owner") != "shujie"
        or value.get("recovery_owner") != "shujie"
        or not HEX64.fullmatch(str(value.get("inventory_sha256", "")))
        or not HEX64.fullmatch(str(value.get("candidate_image_digest", "")))
    ):
        raise ValueError("Complete same-window writer exclusion attestation required")
    stopped = value.get("stopped_container_ids")
    if (
        not isinstance(stopped, list)
        or not stopped
        or any(not isinstance(v, str) or not HEX64.fullmatch(v) for v in stopped)
        or len(set(stopped)) != len(stopped)
    ):
        raise ValueError("Exact stopped writer container identities required")
    if current:
        current_receipt(value)


def row_digest(values: list[str | None]) -> bytes:
    result = hashlib.sha256()
    for value in values:
        if value is None:
            result.update(b"\x00")
        else:
            raw = bytes.fromhex(value)
            result.update(b"\x01" + len(raw).to_bytes(8, "big") + raw)
    return result.digest()


def snapshot(conn: Connection) -> dict[str, Any]:
    """Caller owns a consistent snapshot and transaction cleanup. Never writes SQL."""
    from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema

    started = time.monotonic()
    names = sorted(sa.inspect(conn).get_table_names())
    heads = list(conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalars())
    if heads == [PREVIOUS] and TARGET not in names:
        from qs_ai.maintenance.schema_refactor.validation import require_pending_publisher_drop

        require_pending_publisher_drop(conn)
        state = "43_drop_committed_ledger_pending"
    else:
        require_runtime_schema(conn)
        state = "44_complete" if heads == [HEAD] else "43_intact"
    uuid, database = conn.execute(sa.text("SELECT @@server_uuid,DATABASE()")).one()
    target: dict[str, Any] = {"present": TARGET in names}
    retained: dict[str, Any] = {}
    remaining_rows, remaining_bytes = 20_000, 134_217_728
    quote = conn.dialect.identifier_preparer.quote_identifier
    for name in names:
        if time.monotonic() - started > 60:
            raise ValueError("Protection snapshot time budget exceeded")
        ddl = str(conn.execute(sa.text(f"SHOW CREATE TABLE {quote(name)}")).one()[1])
        columns = [c["name"] for c in sa.inspect(conn).get_columns(name)]
        expressions = [f"CAST({quote(c)} AS BINARY)" for c in columns]
        size = "+".join(f"COALESCE(OCTET_LENGTH({v}),0)" for v in expressions)
        count, length = conn.execute(
            sa.text(f"SELECT COUNT(*),COALESCE(SUM({size}),0) FROM {quote(name)}")
        ).one()
        remaining_rows -= int(count)
        remaining_bytes -= int(length)
        if remaining_rows < 0 or remaining_bytes < 0:
            raise ValueError("Protection snapshot row/byte budget exceeded")
        hashes = [
            row_digest(list(row))
            for row in conn.execute(
                sa.text(f"SELECT {','.join(f'HEX({v})' for v in expressions)} FROM {quote(name)}")
            )
        ]
        if len(hashes) != count:
            raise ValueError("Snapshot count changed")
        item = {
            "columns": columns,
            "rows": int(count),
            "data_bytes": int(length),
            "ddl_sha256": digest(ddl.encode()),
            "rows_sha256": digest(b"".join(sorted(hashes))),
        }
        if name == TARGET:
            target.update(item, ddl=ddl)
        elif name != "alembic_version":
            retained[name] = item
        else:
            ledger = dict(item, ddl=ddl, heads=heads)
    if time.monotonic() - started > 60:
        raise ValueError("Protection snapshot time budget exceeded")
    require_runtime_schema(conn) if state != "43_drop_committed_ledger_pending" else (
        require_pending_publisher_drop(conn)
    )
    return {
        "schema": "ai-publisher-snapshot/v1",
        "state": state,
        "mysql_identity_sha256": digest(encoded([str(uuid), str(database)])),
        "mysql_version": str(conn.scalar(sa.text("SELECT VERSION()"))),
        "head": heads[0],
        "target": target,
        "ledger": ledger,
        "retained": retained,
        "read_only": True,
        "writers_excluded": False,
        "execution_approval": False,
        "captured_at": datetime.now(UTC).isoformat(),
        "fingerprint_codec": "mysql-binary-null-length-rowhash-sorted/v1",
        "budget": {"rows": 20_000, "bytes": 134_217_728, "seconds": 60},
    }


def same_retained(before: dict[str, Any], after: dict[str, Any]) -> None:
    if (
        before["mysql_identity_sha256"] != after["mysql_identity_sha256"]
        or before["mysql_version"] != after["mysql_version"]
        or before["retained"] != after["retained"]
        or before["ledger"]["ddl_sha256"] != after["ledger"]["ddl_sha256"]
        or before["fingerprint_codec"] != after["fingerprint_codec"]
    ):
        raise ValueError("Protected database, definitions or retained original bytes differ")
