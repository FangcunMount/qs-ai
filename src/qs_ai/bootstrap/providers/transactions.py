"""Application-owned SDK resource; its original SQL pool and loop stay host-owned."""

import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

from dishka import Provider, Scope, provide
from reliable_messaging.transaction import ShutdownIncompleteError
from sqlalchemy.ext.asyncio import async_sessionmaker

from qs_ai.application.operations.metrics import TransactionMetrics, TransactionMetricsSnapshot
from qs_ai.bootstrap.messaging_config import read_key
from qs_ai.bootstrap.transaction_messaging import make_transaction_sender
from qs_ai.bootstrap.transaction_services import TransactionServices
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive


class _NoTransactionMetrics:
    def transaction_metrics_snapshot(self) -> TransactionMetricsSnapshot | None:
        return None


class TransactionProvider(Provider):
    @provide(scope=Scope.APP)
    def metrics(self, services: TransactionServices | None) -> TransactionMetrics:
        return services.runtime if services is not None else _NoTransactionMetrics()

    @provide(scope=Scope.APP)
    async def services(
        self, settings: Settings, database: Database
    ) -> AsyncIterator[TransactionServices | None]:
        options = settings.messaging
        if not options.transactions:
            yield None
            return
        if not options.enabled or database.engine is None:
            raise ValueError("Configured transaction workflows require enabled MQ and primary SQL")
        if not options.signing_key_file or not options.qs_recipient_key_file:
            raise ValueError("Configured transaction workflow keys required")
        signing, recipient = await asyncio.gather(
            asyncio.to_thread(read_key, options.signing_key_file, private=True),
            asyncio.to_thread(read_key, options.qs_recipient_key_file, private=False),
        )
        # This factory borrows the exact application's primary engine. Creating
        # either SDK object opens no session, transport or background thread.
        sessions = async_sessionmaker(database.engine, expire_on_commit=False)
        runtime = make_transaction_sender(options, sessions)
        try:
            yield TransactionServices(
                runtime,
                TransactionArchive(sessions, runtime.store.binding),
                signing,
                recipient,
                "evaluation:" + str(uuid4()),
                options.transactions["ai.mysql"].limits.check_timeout,
            )
        finally:
            try:
                await runtime.close(grace_seconds=0)
            except ShutdownIncompleteError:
                # Never dispose the host pool/loop while a real SDK operation
                # still owns it. The application's ordinary shutdown calls
                # Close first; this also covers failure before transport Start.
                await runtime.wait()
