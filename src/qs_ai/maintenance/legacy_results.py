"""Reviewed, bounded legacy result transfer using the original host transaction."""

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.workflow_transport.messaging import valid_hash, valid_id
from qs_ai.infrastructure.workflow_transport.state_events import StateEventRecorder

REVISION = "qs-ai-legacy-results/v1"
SCHEMA_HEAD = "0038_messaging_observations"
MAX_BATCH = 20


class HandoffError(ValueError):
    """Fixed maintenance classification; never includes a body or database diagnostic."""


class ApplyError(HandoffError):
    def __init__(self, result: dict[str, Any]) -> None:
        super().__init__("Legacy result apply stopped; retain partial outcome")
        self.result = result


def digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or (
        value.tzinfo is not None and value.utcoffset() != UTC.utcoffset(value)
    ):
        raise HandoffError("Original result time is invalid")
    return value.replace(tzinfo=UTC).isoformat(timespec="microseconds")


def source(row: Mapping[Any, Any]) -> dict[str, Any]:
    # MySQL JSON has already canonicalized its representation. This is a source
    # snapshot hash, distinct from the original event's transport-body and wire hashes.
    return {
        "event_id": row["event_id"],
        "session_id": row["session_id"],
        "version": row["version"],
        "payload_json_sha256": digest(row["payload"]),
        "attempts": row["attempts"],
        "available_at": timestamp(row["available_at"]),
        "created_at": timestamp(row["created_at"]),
    }


def identities(values: Sequence[str]) -> list[str]:
    if (
        not isinstance(values, Sequence)
        or isinstance(values, (str, bytes))
        or not 1 <= len(values) <= MAX_BATCH
        or any(not isinstance(v, str) or not valid_id(v) for v in values)
        or len(set(values)) != len(values)
    ):
        raise HandoffError("Require 1..20 distinct canonical original event identities")
    return list(values)


async def header(db: AsyncSession) -> dict[str, Any]:
    database = await db.scalar(sa.text("SELECT DATABASE()"))
    heads = list((await db.execute(sa.text("SELECT version_num FROM alembic_version"))).scalars())
    if not database or heads != [SCHEMA_HEAD]:
        raise HandoffError("Reviewed original database and compatible schema required")
    return {"database": database, "schema_head": heads[0]}


async def first_wire(db: AsyncSession, event_id: str, *, lock: bool) -> str | None:
    statement = sa.select(outbox.c.wire, outbox.c.wire_sha256).where(
        outbox.c.producer == "qs-ai",
        outbox.c.destination == "qs-server",
        outbox.c.message_id == event_id,
    )
    if lock:
        statement = statement.with_for_update()
    first = (await db.execute(statement)).one_or_none()
    if first is None:
        return None
    if (
        not isinstance(first.wire, bytes)
        or hashlib.sha256(first.wire).hexdigest() != first.wire_sha256
    ):
        raise HandoffError("First durable wire is damaged")
    return str(first.wire_sha256)


def validate_manifest(value: Any, reviewed_digest: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"revision", "header", "rows", "digest"}:
        raise HandoffError("Reviewed manifest structure invalid")
    original = {k: value[k] for k in ("revision", "header", "rows")}
    if (
        value["revision"] != REVISION
        or not isinstance(reviewed_digest, str)
        or not valid_hash(reviewed_digest)
        or value["digest"] != reviewed_digest
        or digest(original) != reviewed_digest
        or not isinstance(value["header"], dict)
        or set(value["header"]) != {"database", "schema_head"}
        or not isinstance(value["rows"], list)
    ):
        raise HandoffError("Reviewed manifest digest or header invalid")
    ids: list[str] = []
    fields = {
        "event_id",
        "session_id",
        "version",
        "payload_json_sha256",
        "attempts",
        "available_at",
        "created_at",
    }
    for row in value["rows"]:
        if (
            not isinstance(row, dict)
            or set(row) != {"source", "source_sha256", "delivered", "mq_owned", "first_wire_sha256"}
            or not isinstance(row["source"], dict)
            or set(row["source"]) != fields
            or digest(row["source"]) != row["source_sha256"]
            or type(row["delivered"]) is not bool
            or type(row["mq_owned"]) is not bool
            or (
                row["first_wire_sha256"] is not None
                and (
                    not isinstance(row["first_wire_sha256"], str)
                    or not valid_hash(row["first_wire_sha256"])
                )
            )
        ):
            raise HandoffError("Reviewed manifest source invalid")
        ids.append(row["source"]["event_id"])
    identities(ids)
    return value


class ResultHandoff:
    def __init__(
        self, transactions: Transactions, recorder: StateEventRecorder | None = None
    ) -> None:
        self.transactions, self.recorder = transactions, recorder

    async def dry_run(self, event_ids: Sequence[str]) -> dict[str, Any]:
        ids = identities(event_ids)
        rows: list[dict[str, Any]] = []
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await db.execute(sa.text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
            baseline = await header(db)
            for event_id in ids:
                row = (
                    (
                        await db.execute(
                            sa.select(result_outbox).where(result_outbox.c.event_id == event_id)
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise HandoffError("Reviewed original result is missing")
                snapshot = source(row)
                rows.append(
                    {
                        "source": snapshot,
                        "source_sha256": digest(snapshot),
                        "delivered": bool(row["delivered"]),
                        "mq_owned": bool(row["mq_owned"]),
                        "first_wire_sha256": await first_wire(db, event_id, lock=False),
                    }
                )
        manifest = {"revision": REVISION, "header": baseline, "rows": rows}
        return {**manifest, "digest": digest(manifest)}

    async def apply(
        self, value: Any, reviewed_digest: str, *, all_claimers_stopped_and_admission_closed: bool
    ) -> dict[str, Any]:
        manifest = validate_manifest(value, reviewed_digest)
        if all_claimers_stopped_and_admission_closed is not True or self.recorder is None:
            raise HandoffError("Explicit stopped claimers, closed admission and recorder required")
        result: dict[str, Any] = {"digest": reviewed_digest, "complete": False, "rows": []}
        for reviewed in manifest["rows"]:
            event_id = reviewed["source"]["event_id"]
            committing = False
            try:
                async with self.transactions.open() as db:
                    await db.begin()
                    await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
                    if await header(db) != manifest["header"]:
                        raise HandoffError("Original database or schema changed")
                    row = (
                        (
                            await db.execute(
                                sa.select(result_outbox)
                                .where(result_outbox.c.event_id == event_id)
                                .with_for_update()
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if row is None or source(row) != reviewed["source"]:
                        raise HandoffError("Original result source changed")
                    # Ownership/ACK may advance on a repeat. They cannot regress,
                    # and an old gRPC sender cannot silently change an unowned row.
                    if (
                        (reviewed["mq_owned"] and not row["mq_owned"])
                        or (reviewed["delivered"] and not row["delivered"])
                        or (not row["mq_owned"] and bool(row["delivered"]) != reviewed["delivered"])
                    ):
                        raise HandoffError("Original result ownership changed unexpectedly")
                    wire = await first_wire(db, event_id, lock=True)
                    if (
                        reviewed["first_wire_sha256"] is not None
                        and wire != reviewed["first_wire_sha256"]
                    ):
                        raise HandoffError("First reviewed wire changed")
                    transferred = await self.recorder.record_legacy_interpretation(db, row)
                    wire = await first_wire(db, event_id, lock=True)
                    committing = True
                    await db.commit()
                result["rows"].append(
                    {
                        "event_id": event_id,
                        "status": "transferred" if transferred else "retained",
                        "wire_sha256": wire,
                    }
                )
            except Exception:
                result["rows"].append(
                    {"event_id": event_id, "status": "commit_unknown" if committing else "stopped"}
                )
                raise ApplyError(result) from None
        result["complete"] = True
        return result
