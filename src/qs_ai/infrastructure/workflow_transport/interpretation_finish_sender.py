"""One original finish, including first-result replay; no model retry authority."""

import asyncio
import math
from typing import Any

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    LocalOutcome,
    RetryAuthorization,
    State,
    decide_local,
)

from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.interpretation_finish_state import (
    InterpretationFinishInput,
    InterpretationFinishPlanner,
    InterpretationFinishWrite,
    finish_request_id,
    read_finish_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    InterpretationStateIntent,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_root import (
    InterpretationStateRoot,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class InterpretationFinishPending(RuntimeError):
    """Exact finish evidence is unresolved; the caller must not repeat model work."""


class InterpretationFinishSender:
    def __init__(
        self,
        store: MySQLExecutionStore,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive interpretation database budget required")
        self._store, self._archives, self._runtime = store, archives, runtime
        self._database_seconds = database_seconds
        self._planner = InterpretationFinishPlanner(
            store,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def _committed(
        self,
        incoming: InterpretationFinishInput,
        intent: InterpretationStateIntent,
        owned: Execution,
    ) -> None:
        _, first, _ = read_finish_plan(incoming, intent, owned.plan)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple([await self._runtime.store.evidence(ref) for ref in owned.references])
            if decide_local(owned.references, evidence) != LocalOutcome.COMMITTED:
                raise InterpretationFinishPending("Exact original finish witness unavailable")
            if await self._archives.read_committed(first.envelope().message_id) != first:
                raise InterpretationFinishPending("Exact original finish archive unavailable")

    async def __call__(self, claim: Claim, result: WorkflowResult) -> None:
        incoming = InterpretationFinishInput(claim, result)
        async with asyncio.timeout(self._database_seconds):
            intent = incoming.intent(await finish_request_id(self._store, incoming))
            reserved = await self._runtime.store.lookup(intent.request)
        if reserved is None:
            winner = await self._planner.candidate(incoming, intent)
            reserved = await self._runtime.reserve(intent.request, winner)
        if not reserved.acquired:
            if reserved.state == State.COMMITTED:
                return await self._committed(incoming, intent, reserved.execution)
            raise InterpretationFinishPending("Original finish is already owned or unresolved")
        return await self._execute_owned(incoming, intent, reserved.execution)

    async def retry(self, claim: Claim, result: WorkflowResult, previous: Execution) -> None:
        """Explicit host finalization decision; not model or automatic retry authority."""
        incoming = InterpretationFinishInput(claim, result)
        async with asyncio.timeout(self._database_seconds):
            intent = incoming.intent(await finish_request_id(self._store, incoming))
        await self._planner.verify(incoming, intent, previous.plan)
        reserved = await self._runtime.retry(RetryAuthorization(previous))
        if not reserved.acquired:
            raise InterpretationFinishPending("Explicit finish retry was not acquired")
        return await self._execute_owned(incoming, intent, reserved.execution)

    async def _execute_owned(
        self,
        incoming: InterpretationFinishInput,
        intent: InterpretationStateIntent,
        owned: Execution,
    ) -> None:
        business = InterpretationFinishWrite(self._store, incoming, intent, owned.plan)
        root = InterpretationStateRoot(
            self._store.transactions,
            self._archives,
            self._runtime.store,
            intent,
            owned.plan,
            business,
            database_seconds=self._database_seconds,
        )
        try:
            outcome = await self._runtime.execute(owned, root)
        except TransactionSendError as error:
            outcome = error.result
            if outcome.local != LocalOutcome.COMMITTED:
                raise
        if outcome.local != LocalOutcome.COMMITTED:
            raise InterpretationFinishPending("Original finish root is not confirmed committed")
        await self._committed(incoming, intent, owned)
