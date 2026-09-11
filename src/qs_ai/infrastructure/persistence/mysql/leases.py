from dataclasses import dataclass

from sqlalchemy import text

from qs_ai.infrastructure.persistence.mysql.database import Transactions


class LeaseLost(RuntimeError):
    """The operation no longer owns the checkpoint write lease."""


@dataclass(frozen=True)
class Lease:
    thread_id: str
    fence: int


class CheckpointLeases:
    """Technical prototype; acquisition uses database time and a permanent fence row."""

    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def acquire(self, thread_id: str, ttl_seconds: int = 30) -> Lease | None:
        if not thread_id or len(thread_id) > 191 or ttl_seconds <= 0:
            raise ValueError("Invalid checkpoint lease parameters")
        async with self.transactions.open() as session:
            await session.execute(
                text(
                    "INSERT INTO checkpoint_leases (thread_id, fence, expires_at) "
                    "VALUES (:id, 0, '1970-01-01') "
                    "ON DUPLICATE KEY UPDATE thread_id = checkpoint_leases.thread_id"
                ),
                {"id": thread_id},
            )
            row = (
                await session.execute(
                    text(
                        "SELECT fence, expires_at <= UTC_TIMESTAMP(6) AS expired "
                        "FROM checkpoint_leases WHERE thread_id = :id FOR UPDATE"
                    ),
                    {"id": thread_id},
                )
            ).one()
            if not row.expired:
                return None
            fence = int(row.fence) + 1
            await session.execute(
                text(
                    "UPDATE checkpoint_leases SET fence = :fence, "
                    "expires_at = TIMESTAMPADD(SECOND, :ttl, UTC_TIMESTAMP(6)) "
                    "WHERE thread_id = :id"
                ),
                {"id": thread_id, "fence": fence, "ttl": ttl_seconds},
            )
            await session.commit()
            return Lease(thread_id, fence)

    async def release(self, lease: Lease) -> None:
        async with self.transactions.open() as session:
            await session.execute(
                text(
                    "UPDATE checkpoint_leases SET expires_at = '1970-01-01' "
                    "WHERE thread_id = :id AND fence = :fence"
                ),
                {"id": lease.thread_id, "fence": lease.fence},
            )
            await session.commit()
