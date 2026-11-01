"""Single-table reviewed migration guard. No publisher or service startup."""

import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.maintenance.publisher_material import (
    HEAD,
    HEX64,
    PREVIOUS,
    current_receipt,
    digest,
    document,
    encoded,
    same_retained,
    snapshot,
    write_private,
    writer_receipt,
)
from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema

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


def _review(
    path: str,
    reviewed_sha: str,
    identity: str,
    ddl: str,
    *,
    current: bool = True,
) -> dict[str, Any]:
    plan = document(path, reviewed_sha)
    if set(plan) != FIELDS:
        raise ValueError("Exact empty-publisher review fields required")
    source = os.environ.get("QS_AI_RELEASE_SHA", "")
    if (
        not re.fullmatch(r"[0-9a-f]{40}", source)
        or plan["schema"] != "ai-empty-publisher-review/v2"
        or plan["source_sha"] != source
        or plan["initial_head"] != PREVIOUS
        or plan["target_head"] != HEAD
        or plan["mysql_identity_sha256"] != identity
        or plan["table_ddl_sha256"] != ddl
        or plan["writers_excluded"] is not True
        or plan["data_owner"] != "shujie"
        or plan["recovery_owner"] != "shujie"
        or not isinstance(plan["operation_id"], str)
        or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", plan["operation_id"])
    ):
        raise ValueError("Reviewed source, database, layout, owners or writer exclusion differs")
    expiry = datetime.fromisoformat(plan["expires_at"])
    now = datetime.now(UTC)
    if expiry.tzinfo is None or current and not now < expiry <= now + timedelta(hours=1):
        raise ValueError("Current bounded approval required")
    protection = document(plan["protection_manifest"], plan["protection_sha256"])
    if (
        protection.get("schema") != "ai-publisher-protection/v1"
        or protection.get("source_sha") != source
        or protection.get("operation_id") != plan["operation_id"]
        or protection.get("frozen") is not True
        or protection.get("consistency_basis") != "snapshot-plus-owner-writer-attestation"
    ):
        raise ValueError("Frozen same-window protection required; live capture is insufficient")
    frozen = protection["snapshot"]
    if (
        frozen["schema"] != "ai-publisher-snapshot/v1"
        or frozen["state"] != "43_intact"
        or frozen["head"] != PREVIOUS
        or frozen["mysql_identity_sha256"] != identity
        or frozen["target"]["present"] is not True
        or type(frozen["target"]["rows"]) is not int
        or frozen["target"]["rows"] != 0
        or frozen["target"]["ddl_sha256"] != ddl
        or digest(frozen["target"]["ddl"].encode()) != ddl
        or not frozen["retained"]
        or frozen["ledger"]["heads"] != [PREVIOUS]
    ):
        raise ValueError("Exact original empty target, ledger and protected catalog required")
    writer = document(protection["writer_receipt"], protection["writer_sha256"])
    writer_receipt(
        writer, source=source, operation=plan["operation_id"], identity=identity, current=current
    )
    captured = datetime.fromisoformat(frozen["captured_at"])
    issued = datetime.fromisoformat(writer["recorded_at"])
    if captured.tzinfo is None or not issued <= captured <= datetime.fromisoformat(
        writer["expires_at"]
    ):
        raise ValueError("Protection predates this writer exclusion window")
    if current:
        current_receipt(writer)
    restore = document(plan["restore_receipt"], plan["restore_sha256"])
    if (
        restore.get("schema") != "ai-publisher-restore-qualification/v1"
        or restore.get("source_sha") != source
        or restore.get("table_ddl_sha256") != ddl
        or restore.get("isolated") is not True
        or type(restore.get("restored_rows")) is not int
        or restore["restored_rows"] != 0
        or any(
            restore.get(k) is not True
            for k in (
                "ddl_roundtrip_equal",
                "retained_unchanged",
                "head43_to44_passed",
                "unknown_ddl_convergence_passed",
            )
        )
        or not HEX64.fullmatch(str(restore.get("evidence_sha256", "")))
    ):
        raise ValueError(
            "Bound original-target isolated restore and forward qualification required"
        )
    return plan


def retire(
    conn: Connection,
    *,
    fresh: bool,
    review_path: str | None,
    reviewed_sha256: str | None,
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
    identity = digest(encoded([str(uuid), str(database)]))
    ddl = str(conn.execute(sa.text("SHOW CREATE TABLE messaging_outbox")).one()[1])
    # Refuse a nonempty original even before reading approval; recheck under lock below.
    if conn.execute(sa.text("SELECT 1 FROM messaging_outbox LIMIT 1")).first() is not None:
        raise ValueError("Nonempty publisher remains protected")
    if not fresh:
        assert review_path is not None and reviewed_sha256 is not None
        plan = _review(review_path, reviewed_sha256, identity, digest(ddl.encode()))
        frozen = document(plan["protection_manifest"], plan["protection_sha256"])["snapshot"]
        same_retained(frozen, snapshot(conn))
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
                _review(review_path, reviewed_sha256, identity, digest(ddl.encode()))
                # Durable host-mounted directory, consumed before DDL dispatch.
                write_private(review_path + ".consumed", (reviewed_sha256 + "\n").encode())
            conn.execute(sa.text("DROP TABLE messaging_outbox"))
        finally:
            conn.execute(sa.text("UNLOCK TABLES"))
    finally:
        conn.execute(sa.text(f"SET SESSION lock_wait_timeout={wait}"))
