"""Reviewed initial binding only; no DDL, service, SDK producer or business writes."""

import argparse
import asyncio
import hashlib
import json
import os
import re
import stat
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from qs_ai.infrastructure.persistence.mysql.tls import create_mysql_engine
from qs_ai.maintenance.schema_refactor.validation import require_runtime_schema

SCHEMA = "qs-ai-initial-transaction-binding/v1"
HEAD = "0043_native_transaction_limit"
TABLES = (
    "rm_tx_binding",
    "rm_tx_request",
    "rm_tx_execution",
    "rm_tx_witness",
    "messaging_archive",
    "messaging_event_receipts",
    "messaging_native_limit_notices",
)
FIELDS = {
    "schema",
    "source_sha",
    "operation_id",
    "owner",
    "store_id",
    "writer_epoch",
    "route_revision",
    "expires_at",
    "database",
    "identity_sha256",
    "ddl_sha256",
    "migration_resources_sha256",
    "schema_head",
}
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def identity(value: str) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode()) <= 128
        or value.strip() != value
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError("Explicit bounded original identity required")
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def migration_digest() -> str:
    files = sorted(Path("migrations/versions").glob("*.py"))
    if not files or not Path("migrations/versions/0043_native_transaction_limit.py").is_file():
        raise ValueError("Run the exact release image from its application directory")
    return digest({f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in files})


def validate_plan(plan: Any, source: str, now: datetime) -> dict[str, Any]:
    if not isinstance(plan, dict) or set(plan) != FIELDS or not HEX40.fullmatch(source):
        raise ValueError("Exact source and manifest fields required")
    if (
        plan["schema"] != SCHEMA
        or plan["source_sha"] != source
        or plan["owner"] != "ai.mysql"
        or plan["route_revision"] != "m3-1"
        or plan["schema_head"] != HEAD
    ):
        raise ValueError("Initial binding contract mismatch")
    for field in ("operation_id", "store_id", "writer_epoch", "database"):
        identity(plan[field])
    for field in ("identity_sha256", "ddl_sha256", "migration_resources_sha256"):
        if (
            not isinstance(plan[field], str)
            or not HEX64.fullmatch(plan[field])
            or plan[field] == "0" * 64
        ):
            raise ValueError("Reviewed nonzero evidence digest required")
    if not isinstance(plan["expires_at"], str):
        raise ValueError("Bounded UTC expiry required")
    expiry = datetime.fromisoformat(plan["expires_at"])
    if expiry.utcoffset() != timedelta(0) or not now < expiry <= now + timedelta(hours=2):
        raise ValueError("Initial binding review expired or outside bounded window")
    return plan


def read_plan(filename: str, approved_digest: str, source: str) -> tuple[dict[str, Any], str]:
    if not HEX64.fullmatch(approved_digest) or approved_digest == "0" * 64:
        raise ValueError("Independent reviewed raw digest required")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate manifest field")
            result[key] = value
        return result

    fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
        ):
            raise ValueError("Owned private 0600 manifest required")
        raw = stream.read(16385)
    if not raw or len(raw) > 16384 or hashlib.sha256(raw).hexdigest() != approved_digest:
        raise ValueError("Exact reviewed raw manifest required")
    return validate_plan(
        json.loads(raw, object_pairs_hook=pairs), source, datetime.now(UTC)
    ), approved_digest


def write_plan(filename: str, plan: dict[str, Any]) -> str:
    raw = (json.dumps(plan, sort_keys=True, indent=2) + "\n").encode()
    fd = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return hashlib.sha256(raw).hexdigest()


async def inspect(conn: AsyncConnection, *, bound: dict[str, Any] | None = None) -> dict[str, str]:
    await conn.run_sync(require_runtime_schema)
    row = (await conn.execute(sa.text("SELECT @@server_uuid,DATABASE()"))).one()
    if not row[0] or not row[1]:
        raise ValueError("Original primary identity required")
    ddl = {}
    for table in TABLES:
        ddl[table] = (await conn.execute(sa.text(f"SHOW CREATE TABLE `{table}`"))).one()[1]
        if table == "rm_tx_binding" and bound is not None:
            rows = (
                await conn.execute(
                    sa.text(
                        "SELECT owner,store_id,writer_epoch,schema_version "
                        "FROM rm_tx_binding LIMIT 2"
                    )
                )
            ).all()
            if rows != [
                (b"ai.mysql", bound["store_id"].encode(), bound["writer_epoch"].encode(), 1)
            ]:
                raise ValueError("Original binding not exactly installed")
        elif (await conn.execute(sa.text(f"SELECT 1 FROM `{table}` LIMIT 1"))).first() is not None:
            raise ValueError("Existing transaction responsibility or binding cannot be replaced")
    return {
        "database": str(row[1]),
        "identity_sha256": digest([str(row[0]), str(row[1])]),
        "schema_head": HEAD,
        "ddl_sha256": digest(ddl),
        "migration_resources_sha256": migration_digest(),
    }


async def perform(engine: AsyncEngine, action: str, plan: dict[str, Any]) -> dict[str, Any]:
    # The maintenance process owns this root and pool. SERIALIZABLE empty-range
    # reads + the primary key select a single installer; no upsert or compensation.
    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="SERIALIZABLE")
        async with conn.begin():
            current = await inspect(conn, bound=plan if action == "verify" else None)
            if action != "preview" and any(plan[k] != v for k, v in current.items()):
                raise ValueError("Original primary/schema/resources drifted after review")
            if action == "apply":
                validate_plan(plan, plan["source_sha"], datetime.now(UTC))
                await conn.execute(
                    sa.text(
                        "INSERT INTO rm_tx_binding(owner,store_id,writer_epoch,schema_version) "
                        "VALUES(:owner,:store,:epoch,1)"
                    ),
                    {
                        "owner": b"ai.mysql",
                        "store": plan["store_id"].encode(),
                        "epoch": plan["writer_epoch"].encode(),
                    },
                )
    # This line is reached only after the owned root commit reply returned.
    return current


async def run(args: argparse.Namespace) -> None:
    source = os.environ.get("QS_AI_RELEASE_SHA", "")
    if not HEX40.fullmatch(source) or not 0 < args.timeout <= 120:
        raise ValueError("Frozen source and bounded deadline required")
    if args.action == "preview":
        if args.reviewed_sha256 or args.writers_excluded:
            raise ValueError("Preview does not consume approval or claim freeze")
        plan = {
            "schema": SCHEMA,
            "source_sha": source,
            "operation_id": identity(args.operation_id),
            "owner": "ai.mysql",
            "store_id": identity(args.store_id),
            "writer_epoch": identity(args.writer_epoch),
            "route_revision": "m3-1",
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        }
        receipt = ""
    else:
        if (
            args.operation_id
            or args.store_id
            or args.writer_epoch
            or (args.action == "apply" and not args.writers_excluded)
        ):
            raise ValueError("Apply requires reviewed original plan and explicit writer exclusion")
        plan, receipt = read_plan(args.plan, args.reviewed_sha256 or "", source)
    url = os.environ.get("QS_AI_DATABASE_URL")
    if not url or not url.startswith("mysql+asyncmy://"):
        raise ValueError("Explicit host MySQL binding required")
    engine = create_mysql_engine(
        url, pool_size=1, max_overflow=0, connect_args={"connect_timeout": 3}, hide_parameters=True
    )
    try:
        async with asyncio.timeout(args.timeout):
            snapshot = await perform(engine, args.action, plan)
        if args.action == "preview":
            plan.update(snapshot)
            validate_plan(plan, source, datetime.now(UTC))
            receipt = write_plan(args.plan, plan)
    finally:
        async with asyncio.timeout(5):
            await engine.dispose()
    print(
        json.dumps(
            {
                "action": args.action,
                "plan_sha256": receipt,
                "completed": True,
                "read_only": args.action != "apply",
                "cutover_authorized": False,
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reviewed initial ai.mysql binding; no production cutover approval"
    )
    parser.add_argument("--action", choices=("preview", "apply", "verify"), required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--reviewed-sha256")
    parser.add_argument("--operation-id", default="")
    parser.add_argument("--store-id", default="")
    parser.add_argument("--writer-epoch", default="")
    parser.add_argument("--writers-excluded", action="store_true")
    parser.add_argument("--timeout", type=float, default=30)
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception:
        print(
            "Initial transaction binding not proven; preserve original evidence and remain stopped",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
