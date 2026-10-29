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
    # Interpretation state history remains the business ACK ledger. Broker
    # confirmation is reported separately by the transaction protocol metrics.
    where = "" if organization_id is None else " AND s.org_id=:org"
    row = (
        (
            await db.execute(
                text(
                    """SELECT /*+ MAX_EXECUTION_TIME(1000) */
                    COUNT(*) AS mq_pending_interpretation_receipts,
                    COALESCE(GREATEST(MAX(TIMESTAMPDIFF(MICROSECOND,o.created_at,:at)),0)
                        /1000000,0) AS mq_oldest_pending_interpretation_receipt_seconds
                    FROM interpretation_result_outbox o
                    JOIN interpretation_sessions s ON s.id=o.session_id
                    WHERE o.mq_owned=1 AND o.delivered=0"""
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
