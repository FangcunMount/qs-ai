"""Explicit host drain maintenance policy; only precise old abort permits SDK recovery."""

import asyncio
from datetime import datetime
from typing import Any

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import State

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_cancellation import read_cancel_drain_source
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.cancellation_drain_sender import (
    CancellationDrainSender,
)
from qs_ai.infrastructure.workflow_transport.cancellation_drain_state import (
    CancellationDrainOperation,
    read_cancellation_drain_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class CancellationDrainRuntime:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        recover_aborted: bool = False,
    ) -> None:
        if type(recover_aborted) is not bool:
            raise ValueError("Explicit host aborted-recovery policy required")
        self.transactions, self.database_seconds = transactions, database_seconds
        self._runtime, self._recover_aborted = runtime, recover_aborted
        self._sender = CancellationDrainSender(
            transactions,
            archives,
            runtime,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def __call__(self, scope: ManagementScope, at: datetime) -> bool:
        if not self._recover_aborted:
            return await self._sender(scope, at)
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            run = await header(db, scope)
            source = await read_cancel_drain_source(db, scope, at, lock=False)
            if source is None:
                return False
            operation = CancellationDrainOperation.from_run(scope, run)
        async with asyncio.timeout(self.database_seconds):
            old = await self._runtime.store.lookup(operation.intent.request)
        if old is not None and old.state == State.ABORTED:
            facts, _, _ = read_cancellation_drain_plan(
                operation, old.execution.plan, route_resolver=self._runtime.routing
            )
            if facts.source_version is None or source.version < facts.source_version:
                raise MessageConflict("Original drain coordinator regressed")
            if source.version == facts.source_version:
                return await self._sender.retry(operation, old.execution)
            return await self._sender.replan(operation, old.execution, source.at)
        return await self._sender(scope, at)
