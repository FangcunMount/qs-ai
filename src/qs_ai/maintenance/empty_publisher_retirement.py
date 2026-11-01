"""Single-table reviewed migration guard. No publisher, recovery or service startup."""

import hashlib
import json
import os
import re
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema

HEAD = "0044_empty_publisher"
PREVIOUS = "0043_native_transaction_limit"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
FIELDS = {
    "schema",
    "source_sha",
    "operation_id",
    "mysql_identity_sha256",
    "table_ddl_sha256",
    "initial_head",
    "target_head",
    "writers_excluded",
    "data_owner",
    "recovery_owner",
    "expires_at",
    "protection_manifest",
    "protection_sha256",
    "restore_receipt",
    "restore_sha256",
}


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            raise ValueError("Duplicate review field")
        result[key] = value
    return result


def _private(path: str, expected: str, limit: int) -> bytes:
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
    if not raw or len(raw) > limit or hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("Reviewed material changed")
    return raw


def _review(path: str, digest: str, identity: str, ddl: str) -> None:
    plan = json.loads(_private(path, digest, 16384), object_pairs_hook=_pairs)
    if not isinstance(plan, dict) or set(plan) != FIELDS:
        raise ValueError("Exact empty-publisher review fields required")
    source = os.environ.get("QS_AI_RELEASE_SHA", "")
    if (
        not re.fullmatch(r"[0-9a-f]{40}", source)
        or plan["schema"] != "ai-empty-publisher-review/v1"
        or plan["source_sha"] != source
        or plan["initial_head"] != PREVIOUS
        or plan["target_head"] != HEAD
        or plan["mysql_identity_sha256"] != identity
        or plan["table_ddl_sha256"] != ddl
        or plan["writers_excluded"] is not True
    ):
        raise ValueError("Reviewed source, database, layout or writer exclusion differs")
    for key in ("operation_id", "data_owner", "recovery_owner"):
        value = plan[key]
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
            raise ValueError("Explicit operation and owners required")
    expiry = datetime.fromisoformat(plan["expires_at"])
    now = datetime.now(UTC)
    if expiry.tzinfo is None or not now < expiry <= now + timedelta(hours=1):
        raise ValueError("Current bounded approval required")
    _private(plan["protection_manifest"], plan["protection_sha256"], 1048576)
    _private(plan["restore_receipt"], plan["restore_sha256"], 1048576)
    # These are approved protection/restore claims, not automatic certification.


def retire(
    conn: Connection, *, fresh: bool, review_path: str | None, reviewed_sha256: str | None
) -> None:
    if list(conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalars()) != [
        PREVIOUS
    ]:
        raise ValueError("Exact0043 source required; unknown DDL result is not retryable")
    require_runtime_schema(conn)
    if fresh and (review_path or reviewed_sha256):
        raise ValueError("Fresh install does not consume existing-store approval")
    if not fresh and (not review_path or not reviewed_sha256):
        raise ValueError("Existing store requires reviewed offline empty-publisher window")
    uuid, database = conn.execute(sa.text("SELECT @@server_uuid,DATABASE()")).one()
    identity = hashlib.sha256(
        json.dumps([str(uuid), str(database)], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    ddl = str(conn.execute(sa.text("SHOW CREATE TABLE messaging_outbox")).one()[1])
    if not fresh:
        assert review_path is not None and reviewed_sha256 is not None
        _review(review_path, reviewed_sha256, identity, hashlib.sha256(ddl.encode()).hexdigest())
    wait = int(conn.scalar(sa.text("SELECT @@SESSION.lock_wait_timeout")))
    conn.execute(sa.text("SET SESSION lock_wait_timeout=5"))
    try:
        conn.execute(sa.text("LOCK TABLES messaging_outbox WRITE"))
        try:
            if str(conn.execute(sa.text("SHOW CREATE TABLE messaging_outbox")).one()[1]) != ddl:
                raise ValueError("Publisher definition changed before lock")
            if conn.execute(sa.text("SELECT 1 FROM messaging_outbox LIMIT 1")).first() is not None:
                raise ValueError("Nonempty publisher remains protected")
            if not fresh:
                assert review_path is not None and reviewed_sha256 is not None
                # Reread every approval/protection byte after acquiring the lock.
                _review(
                    review_path, reviewed_sha256, identity, hashlib.sha256(ddl.encode()).hexdigest()
                )
                # Exclusive durable consumption precedes DDL. Unknown results keep
                # this marker; operators inspect the original result, never replay.
                consumed = review_path + ".consumed"
                fd = os.open(consumed, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write((reviewed_sha256 + "\n").encode())
                    stream.flush()
                    os.fsync(stream.fileno())
                parent = os.open(str(Path(consumed).parent), os.O_RDONLY)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
            conn.execute(sa.text("DROP TABLE messaging_outbox"))
        finally:
            conn.execute(sa.text("UNLOCK TABLES"))
    finally:
        conn.execute(sa.text(f"SET SESSION lock_wait_timeout={wait}"))
