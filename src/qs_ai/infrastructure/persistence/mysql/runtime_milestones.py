"""Small transactional facts; no payloads, heartbeats or exception strings."""

from sqlalchemy import func, literal_column, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import runtime_milestones


async def record(
    db: AsyncSession,
    session_id: str,
    run_id: str,
    kind: str,
    key: str,
    *,
    invocation_id: str | None = None,
    attempt: int | None = None,
    retention_days: int = 30,
) -> None:
    statement = insert(runtime_milestones).values(
        session_id=session_id,
        run_id=run_id,
        kind=kind,
        dedupe_key=key,
        invocation_id=invocation_id,
        attempt=attempt,
        occurred_at=func.utc_timestamp(6),
        expires_at=func.timestampadd(literal_column("DAY"), retention_days, func.utc_timestamp(6)),
    )
    await db.execute(statement.on_duplicate_key_update(dedupe_key=runtime_milestones.c.dedupe_key))


async def prune(transactions: Transactions) -> int:
    # Only expiring diagnostic facts. Calls, results, approvals and publications are never touched.
    async with transactions.open() as db:
        result = await db.execute(
            text(
                "DELETE FROM runtime_milestones WHERE expires_at < UTC_TIMESTAMP(6) "
                "ORDER BY expires_at LIMIT 1000"
            )
        )
        await db.commit()
        return int(getattr(result, "rowcount", 0))
