"""Half messages precede original cancellation drain; duplicates never redo business."""

import asyncio
from datetime import datetime
from typing import Any

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    LocalOutcome,
    ReplanAuthorization,
    RetryAuthorization,
    State,
    decide_local,
)

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.cancellation_drain_state import (
    CancellationDrainOperation,
    CancellationDrainPlanner,
    apply_cancellation_drain,
    read_cancellation_drain_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class CancellationDrainPending(RuntimeError):
    """Existing drain needs exact proof or explicit recovery, not another business execution."""


class CancellationDrainSender:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        self.transactions = transactions
        self._archives, self._runtime = archives, runtime
        self._planner = CancellationDrainPlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def _committed(self, operation: CancellationDrainOperation, execution: Execution) -> bool:
        _, _, archive = read_cancellation_drain_plan(operation, execution.plan)
        async with asyncio.timeout(self._planner.database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise CancellationDrainPending("Exact original drain witness unavailable")
            if await self._archives.read_committed(archive.envelope().message_id) != archive:
                raise CancellationDrainPending("Exact original drain archive unavailable")
        return True

    async def __call__(self, scope: ManagementScope, at: datetime) -> bool:
        operation = await self._planner.observe(scope, at)
        if operation is None:
            return False
        async with asyncio.timeout(self._planner.database_seconds):
            reservation = await self._runtime.store.lookup(operation.intent.request)
        if reservation is None:
            candidate = await self._planner.candidate(operation, at)
            reservation = await self._runtime.reserve(operation.intent.request, candidate)
        if not reservation.acquired:
            if reservation.state == State.COMMITTED:
                return await self._committed(operation, reservation.execution)
            raise CancellationDrainPending("Original drain already owned or unresolved")
        return await self._execute(operation, reservation.execution)

    async def retry(self, operation: CancellationDrainOperation, previous: Execution) -> bool:
        """Host explicitly retries an aborted frozen drain; never a model or cancel request."""
        read_cancellation_drain_plan(operation, previous.plan)
        reservation = await self._runtime.retry(RetryAuthorization(previous))
        if not reservation.acquired:
            raise CancellationDrainPending("Explicit original drain retry not acquired")
        return await self._execute(operation, reservation.execution)

    async def replan(
        self, operation: CancellationDrainOperation, previous: Execution, at: datetime
    ) -> bool:
        """Explicit new source after old abort; original cancel request remains authoritative."""
        read_cancellation_drain_plan(operation, previous.plan)
        candidate = await self._planner.candidate(operation, at)
        reserved = await self._runtime.replan(ReplanAuthorization(previous, candidate))
        if not reserved.acquired:
            raise CancellationDrainPending("Explicit original drain replan not acquired")
        return await self._execute(operation, reserved.execution)

    async def _execute(self, operation: CancellationDrainOperation, execution: Execution) -> bool:
        read_cancellation_drain_plan(operation, execution.plan)
        root = EvaluationStateRoot(
            self.transactions,
            self._archives,
            self._runtime.store,
            operation.intent,
            execution.plan,
            apply_cancellation_drain,
            database_seconds=self._planner.database_seconds,
            isolation_level="REPEATABLE READ",
        )
        try:
            result = await self._runtime.execute(execution, root)
        except TransactionSendError as error:
            result = error.result
            if result.local != LocalOutcome.COMMITTED:
                raise
        if result.local != LocalOutcome.COMMITTED:
            raise CancellationDrainPending("Original drain root not confirmed committed")
        return await self._committed(operation, execution)
