"""Explicit transaction assembly; the host owns Start, admission and DB lifetime."""

import math
import os
from typing import cast

from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
)
from reliable_messaging.transaction import (
    Binding,
    Execution,
    FrozenPlan,
    LocalExecutor,
    Request,
    Reservation,
    Result,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from qs_ai.bootstrap.messaging import read_rocketmq_config
from qs_ai.config import MessagingOptions
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS


class TransactionAdmissionClosed(RuntimeError):
    pass


class TransactionRuntime:
    """Pause only new sends; final StopAdmission and drain remain SDK-owned."""

    def __init__(self, sender: TransactionalSender, *, admitted: bool) -> None:
        self._sender = sender
        self._admitted = admitted
        self._stopping = False

    @property
    def config(self) -> TransactionConfig:
        return self._sender.config

    @property
    def store(self) -> MySQLTransactionStore:
        return cast(MySQLTransactionStore, self._sender.store)

    async def start(self) -> None:
        await self._sender.start()

    def set_admission(self, opened: bool) -> None:
        if self._stopping or (opened and not self._sender.stats()["ready"]):
            raise TransactionAdmissionClosed("Transaction runtime is not ready for admission")
        self._admitted = opened

    async def reserve(self, request: Request, candidate: FrozenPlan) -> Reservation:
        if not self._admitted or self._stopping:
            raise TransactionAdmissionClosed("Host transaction admission closed")
        return await self._sender.reserve(request, candidate)

    async def execute(self, execution: Execution, local: LocalExecutor) -> Result:
        if not self._admitted or self._stopping:
            raise TransactionAdmissionClosed("Host transaction admission closed")
        return await self._sender.execute(execution, local)

    def stop_admission(self) -> None:
        self._admitted = False
        self._stopping = True
        self._sender.stop_admission()

    async def close(self, *, grace_seconds: float = 30) -> None:
        if not math.isfinite(grace_seconds) or grace_seconds < 0:
            raise ValueError("Finite nonnegative transaction shutdown budget required")
        self.stop_admission()
        await self._sender.close(grace_seconds=grace_seconds)

    async def wait(self) -> None:
        await self._sender.wait()

    def stats(self) -> dict[str, int | bool]:
        result = self._sender.stats()
        result["ready"] = bool(result["ready"] and self._admitted and not self._stopping)
        return result


def make_transaction_sender(
    options: MessagingOptions, sessions: async_sessionmaker[AsyncSession]
) -> TransactionRuntime:
    """Borrow a primary session factory without opening a session/thread/client.

    The host gate honors publish_enabled; callers explicitly manage sender
    Start/StopAdmission/Close/Wait before disposing the application's pool/loop.
    No constructor or Start installs the transaction schema.
    """
    if options.provider != "rocketmq" or set(options.transactions) != {"ai.mysql"}:
        raise ValueError("Explicit ai.mysql transaction configuration required")
    if options.rocketmq is None or (
        os.environ.get("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH") != options.rocketmq.ca_file
    ):
        raise ValueError("Transaction client requires the host's configured verified CA")
    binding = options.transactions["ai.mysql"]
    config = read_rocketmq_config(options)
    routes = {
        r.physical_topic: TransactionRoute(r.topic, r.physical_topic, r.route_revision)
        for r in config.routes
        if r.message_type == "TRANSACTION"
        and r.owner == "ai.mysql"
        and r.route_revision == binding.route_revision
        and r.topic == EVENTS
    }
    if not routes:
        raise ValueError("Required ai.mysql transaction event route absent")
    store = MySQLTransactionStore(
        sessions, Binding("ai.mysql", binding.store_id, binding.writer_epoch)
    )
    sender = TransactionalSender(
        TransactionConfig(
            config.endpoint,
            config.access_key,
            config.secret_key,
            "ai.mysql",
            tuple(routes.values()),
            binding.limits,
        ),
        store,
    )
    return TransactionRuntime(sender, admitted=options.publish_enabled)
