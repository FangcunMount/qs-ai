"""One explicitly authorized policy exception; replay uses its first precise evidence."""

import asyncio
from typing import Any

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import Execution, LocalOutcome, State, decide_local

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.contract_recovery_state import (
    ContractRecoveryInput,
    ContractRecoveryPlanner,
    apply_contract_recovery_state,
    read_contract_recovery_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.transaction_root import StateTransactions


class ContractRecoveryPending(RuntimeError):
    """Existing exception needs exact proof or explicit recovery; not new retry authority."""


class ContractRecoverySender:
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
        self._planner = ContractRecoveryPlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )
        self._transactions, self._archives, self._runtime = transactions, archives, runtime
        self._database_seconds = database_seconds

    async def _committed(self, incoming: ContractRecoveryInput, owned: Execution) -> dict[str, Any]:
        _, first, _, archive = read_contract_recovery_plan(incoming, owned.plan)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple([await self._runtime.store.evidence(ref) for ref in owned.references])
            if decide_local(owned.references, evidence) != LocalOutcome.COMMITTED:
                raise ContractRecoveryPending("Exact original policy exception witness unavailable")
            if await self._archives.read_committed(archive.envelope().message_id) != archive:
                raise ContractRecoveryPending("Exact original policy exception archive unavailable")
        return first.response()

    async def __call__(self, incoming: ContractRecoveryInput) -> dict[str, Any]:
        async with asyncio.timeout(self._database_seconds):
            reserved = await self._runtime.store.lookup(incoming.intent.request)
        if reserved is None:
            winner = await self._planner.candidate(incoming)
            reserved = await self._runtime.reserve(incoming.intent.request, winner)
        if not reserved.acquired:
            if reserved.state == State.COMMITTED:
                return await self._committed(incoming, reserved.execution)
            raise ContractRecoveryPending(
                "Original policy exception is already owned or unresolved"
            )
        read_contract_recovery_plan(incoming, reserved.execution.plan)
        root = EvaluationStateRoot(
            self._transactions,
            self._archives,
            self._runtime.store,
            incoming.intent,
            reserved.execution.plan,
            apply_contract_recovery_state,
            database_seconds=self._database_seconds,
            isolation_level="REPEATABLE READ",
        )
        try:
            result = await self._runtime.execute(reserved.execution, root)
        except TransactionSendError as error:
            result = error.result
            if result.local != LocalOutcome.COMMITTED:
                raise
        if result.local != LocalOutcome.COMMITTED:
            raise ContractRecoveryPending(
                "Original policy exception root is not confirmed committed"
            )
        return await self._committed(incoming, reserved.execution)
