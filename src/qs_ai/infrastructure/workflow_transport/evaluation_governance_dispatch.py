"""One authorized governance invocation; no new cross-RPC retry contract."""

import asyncio
import math
from datetime import datetime
from typing import Any

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import LocalOutcome, decide_local

from qs_ai.application.evaluation.execution_mode import (
    SERIAL_RUNTIME_LIMITS,
    EvaluationRuntimeLimits,
)
from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    GovernanceStateOperation,
    GovernanceStatePlanner,
    GovernanceStateRoot,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from qs_ai.infrastructure.workflow_transport.transaction_root import StateTransactions


class GovernanceTransactionPending(RuntimeError):
    """Read original state or resolve existing responsibility; never auto Retry/Replan."""


class GovernanceStateDispatcher:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: StateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive governance evidence budget required")
        self._transactions, self._archives, self._runtime = transactions, archives, runtime
        self._database_seconds, self._runtime_limits = database_seconds, runtime_limits
        self._planner = GovernanceStatePlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def __call__(self, operation: GovernanceStateOperation, at: datetime) -> EvaluationView:
        request = operation.intent.request
        async with asyncio.timeout(self._database_seconds):
            existing = await self._runtime.store.lookup(request)
        if existing is not None:
            # This interface has no durable public operation-result reader.
            # Returning today's Run view would substitute a different response.
            raise GovernanceTransactionPending("Original governance invocation already exists")
        candidate = await self._planner.candidate(operation, at)
        reservation = await self._runtime.reserve(request, candidate)
        if not reservation.acquired:
            raise GovernanceTransactionPending("Original governance invocation is already owned")
        local = GovernanceStateRoot(
            self._transactions,
            self._archives,
            self._runtime.store,
            operation,
            reservation.execution.plan,
            database_seconds=self._database_seconds,
            runtime_limits=self._runtime_limits,
        )
        try:
            result = await self._runtime.execute(reservation.execution, local)
        except TransactionSendError as error:
            result = error.result
            if result.local == LocalOutcome.ABORTED and local.business_error is not None:
                raise local.business_error from None
            if result.local != LocalOutcome.COMMITTED:
                raise
        if result.local != LocalOutcome.COMMITTED:
            if result.local == LocalOutcome.ABORTED and local.business_error is not None:
                raise local.business_error
            raise GovernanceTransactionPending(
                "Original governance root is not confirmed committed"
            )
        _, value = read_evaluation_state_plan(operation.intent, reservation.execution.plan)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple(
                [
                    await self._runtime.store.evidence(ref)
                    for ref in reservation.execution.references
                ]
            )
            if decide_local(reservation.execution.references, evidence) != LocalOutcome.COMMITTED:
                raise GovernanceTransactionPending("Exact original governance witness unavailable")
            if await self._archives.read_committed(value.envelope().message_id) != value:
                raise GovernanceTransactionPending("Exact original governance archive unavailable")
        if local.view is None:
            raise GovernanceTransactionPending("Original governance response unavailable")
        # FinalUnknown remains unresolved Broker responsibility. Never rerun the
        # root or read a later Run to manufacture this invocation's response.
        return local.view
