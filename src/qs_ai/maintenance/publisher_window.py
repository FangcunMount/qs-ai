"""Offline publisher protection and exact ledger-only forward convergence.

Default capture is live read-only qualification. Reconcile is a separate explicit
write command; it never creates/drops a table, restores business rows or runs SDKs.
"""

import argparse
import asyncio
import os
import re
import sys
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.maintenance.empty_publisher_retirement import _review
from qs_ai.maintenance.publisher_material import (
    HEAD,
    PREVIOUS,
    current_receipt,
    document,
    encoded,
    private_bytes,
    same_retained,
    snapshot,
    write_private,
    writer_receipt,
)
from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema


def protection(
    current: dict[str, Any],
    *,
    source: str,
    operation: str,
    writer_path: str | None = None,
    writer_sha: str | None = None,
) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{40}", source) or not re.fullmatch(
        r"[A-Za-z0-9_.:-]{1,128}", operation
    ):
        raise ValueError("Exact source and operation required")
    if bool(writer_path) != bool(writer_sha):
        raise ValueError("Writer receipt path and digest must be paired")
    frozen = False
    if writer_path and writer_sha:
        receipt = document(writer_path, writer_sha)
        writer_receipt(
            receipt, source=source, operation=operation, identity=current["mysql_identity_sha256"]
        )
        captured = datetime.fromisoformat(current["captured_at"])
        if captured < datetime.fromisoformat(receipt["recorded_at"]):
            raise ValueError("Snapshot predates writer exclusion")
        frozen = True
    return {
        "schema": "ai-publisher-protection/v1",
        "source_sha": source,
        "operation_id": operation,
        "snapshot": current,
        "frozen": frozen,
        "consistency_basis": "snapshot-plus-owner-writer-attestation"
        if frozen
        else "live-read-only-snapshot",
        "writer_receipt": writer_path,
        "writer_sha256": writer_sha,
        "execution_approval": False,
        "retained_fingerprints_are_backup": False,
    }


def reconcile(conn: Connection, path: str, reviewed_sha: str) -> dict[str, Any]:
    """Caller commits once. Unknown commit keeps consumption; inspect, never replay."""
    recovery = document(path, reviewed_sha)
    if (
        set(recovery)
        != {
            "schema",
            "source_sha",
            "operation_id",
            "mysql_identity_sha256",
            "original_review",
            "original_sha256",
            "writer_receipt",
            "writer_sha256",
            "data_owner",
            "recovery_owner",
            "recorded_at",
            "expires_at",
        }
        or recovery["schema"] != "ai-publisher-forward-review/v1"
    ):
        raise ValueError("Exact forward recovery review required")
    current_receipt(recovery)
    if (
        recovery["source_sha"] != os.environ.get("QS_AI_RELEASE_SHA")
        or recovery["data_owner"] != "shujie"
        or recovery["recovery_owner"] != "shujie"
    ):
        raise ValueError("Forward source and owners differ")
    writer = document(recovery["writer_receipt"], recovery["writer_sha256"])
    writer_receipt(
        writer,
        source=recovery["source_sha"],
        operation=recovery["operation_id"],
        identity=recovery["mysql_identity_sha256"],
    )
    original = document(recovery["original_review"], recovery["original_sha256"])
    original = _review(
        recovery["original_review"],
        recovery["original_sha256"],
        recovery["mysql_identity_sha256"],
        original["table_ddl_sha256"],
        current=False,
    )
    frozen = document(original["protection_manifest"], original["protection_sha256"])["snapshot"]
    # The original marker is required even when its approval has since expired.
    marker_path = recovery["original_review"] + ".consumed"
    from qs_ai.maintenance.publisher_material import digest

    expected = (recovery["original_sha256"] + "\n").encode()
    if private_bytes(marker_path, digest(expected)) != expected:
        raise ValueError("Original durable consumption differs")
    # Serialize only this ledger row; approved exclusion covers all actual writers.
    heads = list(
        conn.execute(sa.text("SELECT version_num FROM alembic_version FOR UPDATE")).scalars()
    )
    if heads != [PREVIOUS]:
        raise ValueError("Forward convergence requires exact43 pending ledger; inspect44 read-only")
    current = snapshot(conn)
    if current["state"] != "43_drop_committed_ledger_pending":
        raise ValueError("Forward convergence requires proven target absence")
    same_retained(frozen, current)
    write_private(path + ".consumed", (reviewed_sha + "\n").encode())
    result = conn.execute(
        sa.text("UPDATE alembic_version SET version_num=:target WHERE version_num=:previous"),
        {"target": HEAD, "previous": PREVIOUS},
    )
    if result.rowcount != 1:
        raise ValueError("Ledger convergence did not update exactly one row")
    require_runtime_schema(conn)
    return {
        "schema": "ai-publisher-forward-result/v1",
        "head": HEAD,
        "ledger_rows_changed": 1,
        "business_tables_written": False,
        "DDL_executed": False,
        "commit_confirmed": False,
    }


async def run(args: argparse.Namespace) -> int:
    database: Database | None = None
    try:
        url = os.environ.get("QS_AI_PUBLISHER_WINDOW_DATABASE_URL", "")
        if not url.startswith("mysql+asyncmy://"):
            raise ValueError("Explicit original window database required")
        database = Database(url, pool_size=1, max_overflow=0)
        assert database.engine is not None
        async with asyncio.timeout(90), database.engine.connect() as conn:
            await conn.execution_options(isolation_level="REPEATABLE READ")
            if args.command == "reconcile":
                result = await conn.run_sync(
                    lambda c: reconcile(c, args.review, args.reviewed_sha256)
                )
                await conn.commit()
                await conn.run_sync(require_runtime_schema)
                result["commit_confirmed"] = True
            else:
                await conn.execute(sa.text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
                current = await conn.run_sync(snapshot)
                if args.command == "verify":
                    before = document(args.baseline, args.baseline_sha256)
                    same_retained(before["snapshot"], current)
                    result = {
                        "retained_identical": True,
                        "state": current["state"],
                        "read_only": True,
                    }
                else:
                    result = protection(
                        current,
                        source=os.environ.get("QS_AI_RELEASE_SHA", ""),
                        operation=args.operation_id,
                        writer_path=args.writer_receipt,
                        writer_sha=args.writer_sha256,
                    )
            await conn.rollback()
        await database.close()
        database = None
        result["transaction_released"] = True
        result["pool_released"] = True
        write_private(args.output, encoded(result) + b"\n")
        print("Private receipt saved; no business bootstrap or model request started")
        return 0
    except Exception:
        print(
            "Window action unavailable; preserve original receipts and inspect before retry",
            file=sys.stderr,
        )
        return 1
    finally:
        if database is not None:
            await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    capture = subs.add_parser(
        "capture", help="read-only live qualification unless writer receipt supplied"
    )
    capture.add_argument("--operation-id", required=True)
    capture.add_argument("--writer-receipt")
    capture.add_argument("--writer-sha256")
    verify = subs.add_parser(
        "verify", help="read-only retained comparison; never infers an empty publisher"
    )
    verify.add_argument("--baseline", required=True)
    verify.add_argument("--baseline-sha256", required=True)
    forward = subs.add_parser(
        "reconcile", help="explicit separately reviewed ledger-only43-to44 write"
    )
    forward.add_argument("--review", required=True)
    forward.add_argument("--reviewed-sha256", required=True)
    for sub in (capture, verify, forward):
        sub.add_argument("--output", required=True)
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
