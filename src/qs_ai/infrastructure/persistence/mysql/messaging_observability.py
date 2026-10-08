"""Committed MQ state only, borrowed consistent read-only snapshot, fixed names.

This reader never claims unrecorded historical duplicates/payload errors were zero.
No message content or identity is read, and no transaction/resource is owned here.
"""

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.messaging_observations import collect_observations

QUARANTINE_CODES = (
    "identity_conflict",
    "authentication_failed",
    "invalid_wire",
    "invalid_failure_wire",
    "handler_failed",
)


async def collect_messaging_snapshot(
    db: AsyncSession, observed_at: datetime, *, organization_id: int | None = None
) -> dict[str, float]:
    where = "" if organization_id is None else " WHERE organization_id=:org"
    row = (
        (
            await db.execute(
                text(
                    """SELECT /*+ MAX_EXECUTION_TIME(1000) */
                COALESCE(SUM(stage='staged'),0) AS mq_staged_events,
                COALESCE(SUM(stage IN ('staged','awaiting_receipt')
                    AND available_at<=:at),0) AS mq_due_events,
                COALESCE(SUM(stage='awaiting_receipt'),0) AS mq_awaiting_receipt_events,
                COALESCE(SUM(stage='held'),0) AS mq_held_events,
                COALESCE(GREATEST(MAX(CASE WHEN stage='staged' THEN
                    TIMESTAMPDIFF(MICROSECOND,created_at,:at) END),0)/1000000,0)
                    AS mq_oldest_staged_seconds,
                COALESCE(GREATEST(MAX(CASE WHEN stage='awaiting_receipt' THEN
                    TIMESTAMPDIFF(MICROSECOND,created_at,:at) END),0)/1000000,0)
                    AS mq_oldest_awaiting_receipt_seconds
                FROM messaging_outbox"""
                    + where
                ),
                {"at": observed_at, "org": organization_id},
            )
        )
        .mappings()
        .one()
    )
    values = {name: float(value) for name, value in row.items()}
    # Inbox/quarantine have no trusted organization column. They never cross
    # an organization-scoped read, even when the untrusted body claims an org.
    if organization_id is None:
        values["mq_held_commands"] = float(
            await db.scalar(
                text(
                    "SELECT /*+ MAX_EXECUTION_TIME(1000) */ COUNT(*) "
                    "FROM messaging_inbox WHERE decision='held'"
                )
            )
        )
        rows = (
            (
                await db.execute(
                    text(
                        "SELECT /*+ MAX_EXECUTION_TIME(1000) */ code,COUNT(*) n "
                        "FROM messaging_quarantine WHERE code IN "
                        "('identity_conflict','authentication_failed','invalid_wire',"
                        "'invalid_failure_wire','handler_failed') GROUP BY code"
                    )
                )
            )
            .mappings()
            .all()
        )
        counts = {row["code"]: row["n"] for row in rows}
        for code in QUARANTINE_CODES:
            values[f"mq_quarantine_{code}_records"] = float(counts.get(code, 0))
        values.update(await collect_observations(db))
    return values
