"""Original expired claim -> half messages -> guarded release/Unknown, with precise proof."""

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

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.candidate_expiry_state import (
    CandidateExpiryPlanner,
    CandidateExpiryRequest,
    apply_candidate_expiry,
    read_candidate_expiry_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class CandidateExpiryPending(RuntimeError):
    """Original claim responsibility remains unresolved; never model invocation permission."""


class CandidateExpirySender:
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
        self._planner = CandidateExpiryPlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def _committed(self, request: CandidateExpiryRequest, execution: Execution) -> bool:
        _, _, archive = read_candidate_expiry_plan(request, execution.plan)
        async with asyncio.timeout(self._planner.database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise CandidateExpiryPending("Exact original candidate expiry witness unavailable")
            if await self._archives.read_committed(archive.envelope().message_id) != archive:
                raise CandidateExpiryPending("Exact original candidate expiry archive unavailable")
        return True

    async def __call__(self, request: CandidateExpiryRequest, at: datetime) -> bool:
        async with asyncio.timeout(self._planner.database_seconds):
            reserved = await self._runtime.store.lookup(request.intent.request)
        if reserved is None:
            winner = await self._planner.candidate(request, at)
            reserved = await self._runtime.reserve(request.intent.request, winner)
        if not reserved.acquired:
            if reserved.state == State.COMMITTED:
                return await self._committed(request, reserved.execution)
            raise CandidateExpiryPending("Original candidate expiry already owned or unresolved")
        return await self._execute(request, reserved.execution)

    async def retry(self, request: CandidateExpiryRequest, previous: Execution) -> bool:
        """Explicit host finalization recovery through the bounded public SDK gate."""
        read_candidate_expiry_plan(request, previous.plan)
        reserved = await self._runtime.retry(RetryAuthorization(previous))
        if not reserved.acquired:
            raise CandidateExpiryPending("Explicit candidate expiry retry not acquired")
        return await self._execute(request, reserved.execution)

    async def replan(
        self, request: CandidateExpiryRequest, previous: Execution, at: datetime
    ) -> bool:
        """Host authorizes a new source after exact old abort; claim/model identity persists."""
        read_candidate_expiry_plan(request, previous.plan)
        winner = await self._planner.candidate(request, at)
        reserved = await self._runtime.replan(ReplanAuthorization(previous, winner))
        if not reserved.acquired:
            raise CandidateExpiryPending("Explicit candidate expiry replan not acquired")
        return await self._execute(request, reserved.execution)

    async def _execute(self, request: CandidateExpiryRequest, execution: Execution) -> bool:
        read_candidate_expiry_plan(request, execution.plan)
        root = EvaluationStateRoot(
            self.transactions,
            self._archives,
            self._runtime.store,
            request.intent,
            execution.plan,
            apply_candidate_expiry,
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
            raise CandidateExpiryPending("Original candidate expiry root not confirmed committed")
        return await self._committed(request, execution)
