"""Actual worker preflight -> one owner-bound SDK transaction, without business retry."""

import asyncio
import math
from typing import Any, Protocol
from uuid import UUID

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    FrozenPlan,
    LocalExecutor,
    LocalOutcome,
    Request,
    Reservation,
    Result,
    State,
    decide_local,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_preflight_state import (
    PreflightOperation,
    PreflightStatePlanner,
    apply_preflight_state,
    preflight_intent,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot


class StateTransactions(Protocol):
    @property
    def store(self) -> MySQLTransactionStore: ...

    async def reserve(self, request: Request, candidate: FrozenPlan) -> Reservation: ...

    async def execute(self, execution: Execution, local: LocalExecutor) -> Result: ...


class PreflightTransactionPending(RuntimeError):
    """Existing responsibility needs exact proof or explicit recovery, never model redo."""


class PreflightStateDispatcher:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: StateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive preflight evidence budget required")
        self._transactions, self._archives, self._runtime = transactions, archives, runtime
        self._database_seconds = database_seconds
        self._planner = PreflightStatePlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def _committed(self, intent: EvaluationStateIntent, execution: Execution) -> None:
        facts, value = read_evaluation_state_plan(intent, execution.plan)
        PreflightOperation.from_facts(facts)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise PreflightTransactionPending("Exact original preflight witness unavailable")
            if await self._archives.read_committed(value.envelope().message_id) != value:
                raise PreflightTransactionPending("Exact original preflight archive unavailable")

    async def __call__(self, run_id: UUID, version: int, organization_id: int) -> None:
        intent = preflight_intent(run_id, version, organization_id)
        async with asyncio.timeout(self._database_seconds):
            reservation = await self._runtime.store.lookup(intent.request)
        if reservation is None:
            candidate = await self._planner.candidate(intent)
            reservation = await self._runtime.reserve(intent.request, candidate)
        if not reservation.acquired:
            if reservation.state == State.COMMITTED:
                await self._committed(intent, reservation.execution)
                return
            raise PreflightTransactionPending("Existing preflight needs proof or explicit recovery")
        # In particular, do not capture the losing candidate's time or evidence.
        local = EvaluationStateRoot(
            self._transactions,
            self._archives,
            self._runtime.store,
            intent,
            reservation.execution.plan,
            apply_preflight_state,
            database_seconds=self._database_seconds,
        )
        try:
            result = await self._runtime.execute(reservation.execution, local)
        except TransactionSendError as error:
            result = error.result
            if result.local != LocalOutcome.COMMITTED:
                raise
        if result.local != LocalOutcome.COMMITTED:
            raise PreflightTransactionPending("Original preflight root is not confirmed committed")
        # FinalUnknown remains Unknown; this proves local state, not QS receipt.
        await self._committed(intent, reservation.execution)
