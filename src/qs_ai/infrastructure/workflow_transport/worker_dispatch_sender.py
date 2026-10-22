"""Explicit serial worker sender; only fresh committed proof transfers model capacity."""

import asyncio
from dataclasses import replace
from uuid import UUID

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    LocalOutcome,
    ReplanAuthorization,
    State,
    decide_local,
)

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.execution.model_capacity import ModelCapacity
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import emit
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_step import PreparedStep
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions
from qs_ai.infrastructure.workflow_transport.worker_dispatch_state import (
    WorkerDispatchPlanner,
    WorkerDispatchWrite,
    read_worker_dispatch_plan,
    worker_dispatch_intent,
)


class WorkerDispatchPending(RuntimeError):
    """No new provider invocation: exact recovery of the existing responsibility is needed."""


class WorkerDispatchSender:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        schemas: SchemaAssets,
        owner: str,
        planner: WorkerDispatchPlanner,
        *,
        capacity: ModelCapacity | None = None,
    ) -> None:
        if planner.transactions is not transactions or planner.schemas is not schemas:
            raise ValueError("Original worker transaction/schema owners must match")
        self._transactions, self._archives, self._runtime = transactions, archives, runtime
        self._schemas, self._owner, self._planner, self._capacity = (
            schemas,
            owner,
            planner,
            capacity,
        )

    async def __call__(self, run_id: UUID, version: int, organization_id: int) -> PreparedStep:
        return await self._send(run_id, version, organization_id, previous=None)

    async def replan(
        self,
        run_id: UUID,
        version: int,
        organization_id: int,
        previous: Execution,
    ) -> PreparedStep:
        """Only an explicit host decision after exact old abort can acquire a fresh model lease."""
        return await self._send(run_id, version, organization_id, previous=previous)

    async def _send(
        self,
        run_id: UUID,
        version: int,
        organization_id: int,
        *,
        previous: Execution | None,
    ) -> PreparedStep:
        intent = worker_dispatch_intent(run_id, version, organization_id, self._owner)
        async with asyncio.timeout(self._planner.database_seconds):
            existing = await self._runtime.store.lookup(intent.request)
        if previous is None and existing is not None:
            # A commit is not authorization for this new process to invoke a model again.
            raise WorkerDispatchPending("Existing worker dispatch cannot authorize a model retry")
        if previous is not None:
            read_worker_dispatch_plan(intent, previous.plan)
            if (
                existing is None
                or existing.state != State.ABORTED
                or existing.execution.plan.digest != previous.plan.digest
                or existing.execution.fence != previous.fence
                or existing.execution.writer_epoch != previous.writer_epoch
            ):
                raise WorkerDispatchPending("Exact old aborted serial dispatch required")
        candidate, inputs = await self._planner.candidate(
            run_id, version, organization_id, self._owner
        )
        token = None
        if self._capacity is not None:
            token = self._capacity.try_acquire(inputs.route.provider, evaluation=True)
            if token is None:
                raise CheckpointConflict("Model capacity unavailable; defer without dispatch")
        try:
            reservation = (
                await self._runtime.reserve(intent.request, candidate)
                if previous is None
                else await self._runtime.replan(ReplanAuthorization(previous, candidate))
            )
            if not reservation.acquired:
                raise WorkerDispatchPending("Another worker owns the original dispatch")
            if reservation.execution.plan.digest != candidate.digest:
                raise WorkerDispatchPending("Owned worker plan differs from first candidate")
            _, operation, archive = read_worker_dispatch_plan(intent, reservation.execution.plan)
            business = WorkerDispatchWrite(self._schemas)
            root = EvaluationStateRoot(
                self._transactions,
                self._archives,
                self._runtime.store,
                intent,
                reservation.execution.plan,
                business,
                database_seconds=self._planner.database_seconds,
                isolation_level="REPEATABLE READ",
            )
            try:
                result = await self._runtime.execute(reservation.execution, root)
            except TransactionSendError as error:
                result = error.result
                if result.local != LocalOutcome.COMMITTED:
                    raise
            if result.local != LocalOutcome.COMMITTED:
                raise WorkerDispatchPending("Original worker root is not confirmed committed")
            async with asyncio.timeout(self._planner.database_seconds):
                evidence = tuple(
                    [
                        await self._runtime.store.evidence(ref)
                        for ref in reservation.execution.references
                    ]
                )
                if (
                    decide_local(reservation.execution.references, evidence)
                    != LocalOutcome.COMMITTED
                ):
                    raise WorkerDispatchPending("Exact original worker witness unavailable")
                if await self._archives.read_committed(archive.envelope().message_id) != archive:
                    raise WorkerDispatchPending("Exact original worker archive unavailable")
            if business.prepared is None or business.prepared.cp != operation.checkpoint:
                raise WorkerDispatchPending("Original local model handoff unavailable")
            emit(
                "evaluation.model_dispatched",
                "evaluation",
                run_id=str(run_id),
                invocation_id=business.prepared.invocation_id,
                stage=business.prepared.cp.kind,
            )
            prepared = replace(business.prepared, capacity_token=token)
            token = None  # Ownership passes exactly once to the original graph's finally.
            return prepared
        finally:
            if token is not None:
                token.release()


class WorkerDispatchRecovery:
    """Explicit host policy for aborted dispatch before domain claim or model invocation."""

    def __init__(self, sender: WorkerDispatchSender) -> None:
        self.sender = sender

    async def __call__(self, run_id: UUID, version: int, organization_id: int) -> PreparedStep:
        intent = worker_dispatch_intent(run_id, version, organization_id, self.sender._owner)
        async with asyncio.timeout(self.sender._planner.database_seconds):
            old = await self.sender._runtime.store.lookup(intent.request)
        if old is not None and old.state == State.ABORTED:
            return await self.sender.replan(run_id, version, organization_id, old.execution)
        return await self.sender(run_id, version, organization_id)
