"""Permanent read-only inventory, separate from reviewed legacy transfer."""

import argparse
import asyncio
import json
import os
import sys
from typing import Any

import sqlalchemy as sa

from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.workflow_transport.messaging import INLINE_BODY
from qs_ai.maintenance.legacy_results import timestamp
from qs_ai.maintenance.schema_layout import physical_table, require_known_head
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD


async def inventory(transactions: Transactions, *, max_rows: int = 1000) -> dict[str, Any]:
    if type(max_rows) is not int or not 1 <= max_rows <= 10_000:
        raise ValueError("Inventory requires an explicit bounded row limit")
    async with asyncio.timeout(30), transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        await db.execute(sa.text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        database = await db.scalar(sa.text("SELECT DATABASE()"))
        columns: dict[str, set[str]] = {}
        for table, column in await db.execute(
            sa.text(
                "SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE()"
            )
        ):
            columns.setdefault(table, set()).add(column)
        heads = (
            list((await db.execute(sa.text("SELECT version_num FROM alembic_version"))).scalars())
            if "version_num" in columns.get("alembic_version", set())
            else None
        )
        schema_head = require_known_head(heads)
        report: dict[str, Any] = {
            "revision": "qs-ai-messaging-audit/v1",
            "database": database,
            "schema_heads": heads,
            "handoff_schema_compatible": True,
            "complete": True,
            "sections": {},
            "classification": "stored facts only; no recovery or model retry authorization",
            "body_reference_basis": "stored length and current codec threshold; no JWE decryption",
        }

        async def section(
            name: str,
            fields: list[str],
            *,
            extras: str = "",
            required: set[str] | None = None,
            order: str = "",
        ) -> None:
            # All names/expressions are fixed below, never supplied by CLI input.
            table = physical_table(schema_head, name)
            predicate = ""
            if schema_head == NEW_HEAD and name in (
                "evaluation_generation_completions",
                "evaluation_semantic_completions",
            ):
                kind = "generation" if name == "evaluation_generation_completions" else "semantic"
                predicate = f" WHERE kind='{kind}'"
                required = (required or set()) | {"kind"}
            available = columns.get(table)
            entry: dict[str, Any] = {
                "table_exists": available is not None,
                "physical_table": table,
            }
            report["sections"][name] = entry
            if available is None or not set(fields).union(required or set()).issubset(available):
                entry.update(available=False, total=None, rows=None)
                report["complete"] = False
                return
            total = int(await db.scalar(sa.text(f"SELECT COUNT(*) FROM `{table}`{predicate}")) or 0)
            rows = (
                await db.execute(
                    sa.text(
                        f"SELECT {','.join(fields)}{extras} FROM `{table}`{predicate} "
                        f"ORDER BY {order or fields[0]} LIMIT :limit"
                    ),
                    {"limit": max_rows},
                )
            ).mappings()
            values = [
                {k: timestamp(v) if hasattr(v, "isoformat") else v for k, v in row.items()}
                for row in rows
            ]
            entry.update(available=True, total=total, rows=values, truncated=total > max_rows)
            if total > max_rows:
                report["complete"] = False

        owned = (
            "mq_owned"
            if "mq_owned" in columns.get(physical_table(schema_head, "result_outbox"), set())
            else "NULL"
        )
        await section(
            "result_outbox",
            [
                "event_id",
                "session_id",
                "version",
                "delivered",
                "attempts",
                "available_at",
                "created_at",
                "delivered_at",
            ],
            extras=f",{owned} AS mq_owned,SHA2(CAST(payload AS CHAR),256) AS mysql_json_sha256",
            required={"payload"},
        )
        report["sections"]["result_outbox"]["ownership_column_available"] = owned != "NULL"
        await section(
            "ai_messaging_outbox",
            [
                "producer",
                "destination",
                "message_id",
                "kind",
                "aggregate_key",
                "aggregate_sequence",
                "stage",
                "attempts",
                "available_at",
                "created_at",
                "published_at",
                "confirmed_at",
                "body_sha256",
                "wire_sha256",
                "error_code",
            ],
            extras=(
                ",OCTET_LENGTH(body) AS body_bytes,OCTET_LENGTH(wire) AS wire_bytes,"
                "SHA2(body,256)=body_sha256 AS body_hash_matches,"
                "SHA2(wire,256)=wire_sha256 AS wire_hash_matches,"
                f"OCTET_LENGTH(body)>{INLINE_BODY} AS body_reference_expected"
            ),
            required={"body", "wire"},
            order="producer,destination,message_id",
        )
        await section(
            "ai_messaging_inbox",
            [
                "producer",
                "message_id",
                "destination",
                "kind",
                "decision",
                "receipt_id",
                "body_sha256",
                "wire_sha256",
            ],
            order="producer,message_id",
        )
        await section(
            "ai_messaging_quarantine",
            [
                "wire_sha256",
                "code",
                "logical_producer",
                "logical_message_id",
                "logical_body_sha256",
                "attempts",
                "first_seen_at",
                "last_seen_at",
            ],
        )
        await section(
            "model_calls",
            ["run_id", "invocation_id", "status", "failure_code"],
            extras=",response_json IS NOT NULL AS persistent_response_present,"
            "SHA2(request_json,256) AS original_request_sha256",
            required={"request_json", "response_json"},
        )
        await section(
            "evaluation_dispatches",
            ["run_id", "invocation_id", "execution_id", "kind"],
            extras=",JSON_UNQUOTE(JSON_EXTRACT(checkpoint_json,'$.phase')) AS original_phase",
            required={"checkpoint_json"},
            order="run_id,invocation_id",
        )
        for name in ("evaluation_generation_completions", "evaluation_semantic_completions"):
            await section(
                name,
                ["run_id", "execution_id", "invocation_id"],
                extras=(
                    ",JSON_UNQUOTE(JSON_EXTRACT(evidence_json,'$.status')) AS original_status,"
                    "JSON_EXTRACT(evidence_json,'$.provider_call_count') "
                    "AS original_provider_call_count,"
                    "OCTET_LENGTH(raw_output) AS raw_bytes"
                ),
                required={"evidence_json", "raw_output"},
                order="run_id,execution_id",
            )
        return report


async def run(max_rows: int) -> int:
    database: Database | None = None
    try:
        url = os.environ.get("QS_AI_MESSAGING_AUDIT_DATABASE_URL")
        if not url or not url.startswith("mysql+asyncmy://"):
            raise ValueError("Explicit original MySQL audit database required")
        database = Database(url)
        report = await inventory(Transactions(database), max_rows=max_rows)
        print(
            json.dumps(
                {"source_sha": os.environ.get("QS_AI_RELEASE_SHA", "development"), "result": report}
            )
        )
        return 0 if report["complete"] else 2
    except Exception:
        print(
            "Messaging inventory unavailable; no empty or complete state can be inferred",
            file=sys.stderr,
        )
        return 1
    finally:
        if database is not None:
            try:
                await database.close()
            except Exception:
                print("Owned inventory database cleanup failed", file=sys.stderr)
                return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only stored message and dispatch inventory")
    parser.add_argument("--max-rows", type=int, default=1000)
    raise SystemExit(asyncio.run(run(parser.parse_args().max_rows)))


if __name__ == "__main__":
    main()
