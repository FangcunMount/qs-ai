"""One bounded consistent snapshot; only aggregate values cross this adapter."""

import asyncio

from sqlalchemy import text

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging_observability import (
    collect_messaging_snapshot,
)

QUERIES = (
    """SELECT COUNT(*) AS ready_jobs,
        COALESCE(MAX(TIMESTAMPDIFF(MICROSECOND,available_at,UTC_TIMESTAMP(6)))/1000000,0)
          AS oldest_ready_job_seconds
        FROM execution_jobs WHERE status='queued' AND available_at<=UTC_TIMESTAMP(6)""",
    """SELECT COUNT(*) AS expired_job_leases FROM execution_jobs
        WHERE status='leased' AND lease_until<=UTC_TIMESTAMP(6)""",
    """SELECT COUNT(*) AS pending_results,
        COALESCE(GREATEST(MAX(TIMESTAMPDIFF(MICROSECOND,created_at,UTC_TIMESTAMP(6))),0)
          /1000000,0) AS oldest_pending_result_seconds,
        COALESCE(SUM(created_at IS NULL),0) AS pending_results_without_timestamp
        FROM interpretation_result_outbox WHERE delivered=0""",
    """SELECT COUNT(*) AS unresolved_model_calls
        FROM execution_model_calls m JOIN interpretation_sessions s ON s.active_run_id=m.run_id
        WHERE s.status NOT IN ('completed','cancelled') AND
          (m.status='unknown' OR (m.status='dispatched'
           AND m.created_at<UTC_TIMESTAMP(6)-INTERVAL 5 MINUTE))""",
    """SELECT COUNT(*) AS database_account_connections FROM information_schema.PROCESSLIST
        WHERE USER=SUBSTRING_INDEX(CURRENT_USER(),'@',1) AND DB=DATABASE()""",
    """SELECT COUNT(*) AS provider_response_samples_5m,
        COALESCE(MAX(CAST(JSON_UNQUOTE(JSON_EXTRACT(response_json,
          '$.latency_milliseconds')) AS DECIMAL(20,3)))/1000,0)
          AS provider_response_max_seconds_5m
        FROM execution_model_calls WHERE status='response_received' AND response_json IS NOT NULL
          AND created_at>=UTC_TIMESTAMP(6)-INTERVAL 5 MINUTE""",
    """SELECT COUNT(*) AS capacity_rejections_24h FROM interpretation_sessions
        WHERE failure_code='participant_daily_capacity_exceeded'
          AND created_at>=UTC_TIMESTAMP(6)-INTERVAL 1 DAY""",
)


class MySQLOperationalMetrics:
    def __init__(self, transactions: Transactions, settings: Settings) -> None:
        self.transactions = transactions
        self.messaging_enabled = settings.messaging.enabled

    async def collect(self) -> dict[str, float]:
        try:
            async with asyncio.timeout(5):
                async with self.transactions.open() as db:
                    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                    await db.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
                    values = {
                        "database_up": 1.0,
                        "mq_enabled": float(self.messaging_enabled),
                        "mq_observation_available": 0.0,
                    }
                    for query in QUERIES:
                        bounded = query.replace(
                            "SELECT ", "SELECT /*+ MAX_EXECUTION_TIME(1000) */ ", 1
                        )
                        row = (await db.execute(text(bounded))).mappings().one()
                        values.update({name: float(value) for name, value in row.items()})
                    if self.messaging_enabled:
                        observed_at = await db.scalar(text("SELECT UTC_TIMESTAMP(6)"))
                        values.update(await collect_messaging_snapshot(db, observed_at))
                        values["mq_observation_available"] = 1.0
                    return values
        except Exception:
            # No partial/stale metrics and no driver details (DSN/query/body) in HTTP or logs.
            return {
                "database_up": 0.0,
                "mq_enabled": float(self.messaging_enabled),
                "mq_observation_available": 0.0,
            }
