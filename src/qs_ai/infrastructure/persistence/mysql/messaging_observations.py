"""Fixed durable technical observations, never identities, bodies or retry authority.

Duplicate observations borrow the original business transaction. Payload failure
observations use an explicit bounded technical transaction after the read failed;
they cannot change the original read outcome. Counts are recorded facts since
installation, not reconstructed complete history or a new delivery ledger.
"""

import asyncio
import logging

import sqlalchemy as sa
from reliable_messaging.sqlalchemy import bind
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import Transactions

KINDS = (
    "duplicate_command",
    "duplicate_ack",
    "payload_fetch_unavailable",
    "payload_fetch_reference_mismatch",
    "payload_fetch_workload_denied",
    "payload_serve_reference_mismatch",
    "payload_serve_workload_denied",
    "payload_serve_storage_unavailable",
)
MAX_COUNT = 2**64 - 1
metadata = sa.MetaData()
observations = sa.Table(
    "messaging_observations",
    metadata,
    sa.Column("kind", sa.String(64, collation="ascii_bin"), primary_key=True),
    sa.Column("recorded_count", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("recording_since", mysql.DATETIME(fsp=6), nullable=False),
    sa.Column("last_observed_at", mysql.DATETIME(fsp=6)),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)


async def record_observation(db: AsyncSession, kind: str) -> None:
    if kind not in KINDS:
        raise ValueError("fixed MQ observation kind required")
    now = sa.func.utc_timestamp(6)
    statement = mysql.insert(observations).values(
        kind=kind, recorded_count=1, recording_since=now, last_observed_at=now
    )
    await bind(db).append(
        statement.on_duplicate_key_update(
            recorded_count=sa.case(
                (observations.c.recorded_count < MAX_COUNT, observations.c.recorded_count + 1),
                else_=MAX_COUNT,
            ),
            last_observed_at=now,
        )
    )


async def record_payload_failure(transactions: Transactions, kind: str) -> bool:
    if kind not in KINDS or not kind.startswith("payload_"):
        raise ValueError("fixed payload failure kind required")
    try:
        async with asyncio.timeout(1), transactions.open() as db:
            await db.begin()
            await record_observation(db, kind)
            await db.commit()
        return True
    except Exception:
        # Never expose an exception/DSN or mask the original payload failure.
        logging.getLogger(__name__).warning("mq_payload_observation_unavailable")
        return False


async def collect_observations(db: AsyncSession) -> dict[str, float]:
    rows = (
        (
            await db.execute(
                sa.text(
                    "SELECT /*+ MAX_EXECUTION_TIME(1000) */ kind,recorded_count,"
                    "TIMESTAMPDIFF(MICROSECOND,'1970-01-01 00:00:00',recording_since)"
                    "/1000000 AS since_epoch_seconds FROM messaging_observations"
                )
            )
        )
        .mappings()
        .all()
    )
    by_kind = {row["kind"]: row for row in rows if row["kind"] in KINDS}
    # A missing seeded category is unavailable, not a fabricated zero history.
    if set(by_kind) != set(KINDS):
        return {
            "mq_duplicate_observations_available": 0,
            "mq_payload_error_observations_available": 0,
        }
    values = {
        "mq_duplicate_observations_available": 1.0,
        "mq_payload_error_observations_available": 1.0,
        "mq_observations_history_complete": 0.0,
        "mq_observations_recording_since_epoch_seconds": max(
            float(row["since_epoch_seconds"]) for row in by_kind.values()
        ),
    }
    values.update(
        {f"mq_recorded_{kind}": float(row["recorded_count"]) for kind, row in by_kind.items()}
    )
    return values


async def require_recording_schema(db: AsyncSession) -> None:
    try:
        values = await collect_observations(db)
    except sa.exc.SQLAlchemyError:
        raise RuntimeError("MQ required technical observation schema unavailable") from None
    if (
        values.get("mq_duplicate_observations_available") != 1
        or values.get("mq_payload_error_observations_available") != 1
    ):
        raise RuntimeError("MQ required technical observation schema unavailable")
