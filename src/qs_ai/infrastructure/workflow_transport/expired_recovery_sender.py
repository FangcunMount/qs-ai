"""Expired serial ownership converges via original transaction; never resend a provider call."""

import asyncio
from datetime import datetime
from typing import Any
from uuid import UUID

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    LocalOutcome,
    RetryAuthorization,
    State,
    decide_local,
)

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.expired_recovery_state import (
    ExpiredRecoveryPlanner,
    ExpiredRecoveryRequest,
    apply_expired_recovery,
    read_expired_recovery_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class ExpiredRecoveryPending(RuntimeError):
    """Exact prior expiry responsibility remains unresolved; no automatic model retry."""


class ExpiredRecoverySender:
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
        self._planner = ExpiredRecoveryPlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            route_resolver=runtime.routing,
        )

    async def dispatch(
        self,
        run_id: UUID,
        version: int,
        organization_id: int,
        invocation_id: str,
        observed_expiry: datetime,
        at: datetime,
        actor: str,
    ) -> bool:
        """Original bounded scan signature; the scan itself owns no business root."""
        request = ExpiredRecoveryRequest(
            run_id, organization_id, version, invocation_id, observed_expiry
        )
        return await self(request, actor, at)

    async def _committed(self, request: ExpiredRecoveryRequest, execution: Execution) -> bool:
        _, _, archive = read_expired_recovery_plan(
            request, execution.plan, route_resolver=self._runtime.routing
        )
        async with asyncio.timeout(self._planner.database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise ExpiredRecoveryPending("Exact original expired recovery witness unavailable")
            if await self._archives.read_committed(archive.envelope().message_id) != archive:
                raise ExpiredRecoveryPending("Exact original expired recovery archive unavailable")
        return True

    async def __call__(self, request: ExpiredRecoveryRequest, actor: str, at: datetime) -> bool:
        async with asyncio.timeout(self._planner.database_seconds):
            reserved = await self._runtime.store.lookup(request.intent.request)
        if reserved is None:
            winner = await self._planner.candidate(request, actor, at)
            reserved = await self._runtime.reserve(request.intent.request, winner)
        if not reserved.acquired:
            if reserved.state == State.COMMITTED:
                return await self._committed(request, reserved.execution)
            raise ExpiredRecoveryPending("Original expired recovery already owned or unresolved")
        return await self._execute(request, reserved.execution)

    async def retry(self, request: ExpiredRecoveryRequest, previous: Execution) -> bool:
        """Only an explicit host decision can retry the already aborted frozen finalization."""
        read_expired_recovery_plan(request, previous.plan, route_resolver=self._runtime.routing)
        reserved = await self._runtime.retry(RetryAuthorization(previous))
        if not reserved.acquired:
            raise ExpiredRecoveryPending("Explicit original expired recovery retry not acquired")
        return await self._execute(request, reserved.execution)

    async def _execute(self, request: ExpiredRecoveryRequest, execution: Execution) -> bool:
        read_expired_recovery_plan(request, execution.plan, route_resolver=self._runtime.routing)
        root = EvaluationStateRoot(
            self.transactions,
            self._archives,
            self._runtime.store,
            request.intent,
            execution.plan,
            apply_expired_recovery,
            database_seconds=self._planner.database_seconds,
            isolation_level="REPEATABLE READ",
            route_resolver=self._runtime.routing,
        )
        try:
            result = await self._runtime.execute(execution, root)
        except TransactionSendError as error:
            result = error.result
            if result.local != LocalOutcome.COMMITTED:
                raise
        if result.local != LocalOutcome.COMMITTED:
            raise ExpiredRecoveryPending("Original expired recovery root not confirmed committed")
        return await self._committed(request, execution)
