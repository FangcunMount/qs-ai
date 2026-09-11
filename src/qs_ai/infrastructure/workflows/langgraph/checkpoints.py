import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from asyncmy import connect
from asyncmy.connection import Connection
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import ChannelVersions, Checkpoint, CheckpointMetadata
from langgraph.checkpoint.mysql.asyncmy import AsyncMySaver

from qs_ai.infrastructure.persistence.mysql.leases import Lease, LeaseLost


class FencedMySQLSaver(AsyncMySaver):
    """Version-coupled adapter: fence checks and checkpoint writes share one transaction.

    Provision tables separately using AsyncMySaver.setup; never migrate via this saver.
    Every writer of the guarded thread must use this adapter.
    """

    def __init__(self, conn: Connection, lease: Lease) -> None:
        super().__init__(conn)
        self.lease = lease
        self._connection = conn

    def _check_thread(self, thread_id: str | None) -> None:
        if thread_id != self.lease.thread_id:
            raise LeaseLost("Checkpoint thread does not match lease")

    async def _check_fence(self, cursor: Any) -> None:
        await cursor.execute(
            "SELECT fence, expires_at > UTC_TIMESTAMP(6) AS active "
            "FROM checkpoint_leases WHERE thread_id = %s FOR UPDATE",
            (self.lease.thread_id,),
        )
        row = await cursor.fetchone()
        if row is None or row["fence"] != self.lease.fence or not row["active"]:
            raise LeaseLost("Checkpoint write lease expired or superseded")

    @asynccontextmanager
    async def _cursor(self, *, pipeline: bool = False) -> AsyncIterator[Any]:
        async with super()._cursor(pipeline=pipeline) as cursor:
            try:
                if pipeline:
                    await self._check_fence(cursor)
                yield cursor
                if pipeline:
                    # Recheck expiry before commit; the locked row excludes takeover.
                    await self._check_fence(cursor)
            except asyncio.CancelledError:
                # The upstream transaction handler catches Exception, not cancellation.
                if pipeline:
                    await self._connection.rollback()
                raise

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        self._check_thread(config.get("configurable", {}).get("thread_id"))
        return await super().aput(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self._check_thread(config.get("configurable", {}).get("thread_id"))
        await super().aput_writes(config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        self._check_thread(thread_id)
        await super().adelete_thread(thread_id)


@asynccontextmanager
async def guarded_saver(dsn: str, lease: Lease) -> AsyncIterator[FencedMySQLSaver]:
    async with connect(**AsyncMySaver.parse_conn_string(dsn), autocommit=True) as conn:
        yield FencedMySQLSaver(conn, lease)
