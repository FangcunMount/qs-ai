"""Only a fresh local candidate dispatch can transfer the original model-capacity permit."""

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
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import require_claim
from qs_ai.infrastructure.persistence.mysql.evaluation_step import PreparedStep
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_state import (
    CandidateDispatchPlanner,
    CandidateDispatchWrite,
    candidate_dispatch_intent,
    read_candidate_dispatch_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class CandidateDispatchPending(RuntimeError):
    """No provider call is authorized while the first dispatch responsibility is unresolved."""


class CandidateDispatchSender:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        owner: str,
        planner: CandidateDispatchPlanner,
        *,
        capacity: ModelCapacity | None = None,
    ) -> None:
        if planner.transactions is not transactions:
            raise ValueError("Original candidate transaction owners must match")
        self.transactions, self.archives, self.runtime = transactions, archives, runtime
        self.owner, self.planner, self.capacity = owner, planner, capacity

    async def __call__(self, run_id: UUID, version: int, organization_id: int) -> PreparedStep:
        return await self._send(run_id, version, organization_id, previous=None)

    async def replan(
        self,
        run_id: UUID,
        version: int,
        organization_id: int,
        previous: Execution,
    ) -> PreparedStep:
        """Host authorizes a fresh lease only after an exact aborted, uncommitted dispatch."""
        return await self._send(run_id, version, organization_id, previous=previous)

    async def _send(
        self,
        run_id: UUID,
        version: int,
        organization_id: int,
        *,
        previous: Execution | None,
    ) -> PreparedStep:
        intent = candidate_dispatch_intent(run_id, version, organization_id)
        async with asyncio.timeout(self.planner.database_seconds):
            existing = await self.runtime.store.lookup(intent.request)
        if previous is None and existing is not None:
            # Even a committed dispatch is not permission to invoke the provider twice.
            raise CandidateDispatchPending("Existing candidate dispatch requires exact recovery")
        if previous is not None:
            read_candidate_dispatch_plan(intent, previous.plan, route_resolver=self.runtime.routing)
            if (
                existing is None
                or existing.state != State.ABORTED
                or existing.execution.plan.digest != previous.plan.digest
                or existing.execution.fence != previous.fence
                or existing.execution.writer_epoch != previous.writer_epoch
            ):
                raise CandidateDispatchPending("Exact old aborted candidate dispatch required")
        candidate, inputs = await self.planner.candidate(
            run_id, version, organization_id, self.owner
        )
        token = None
        if self.capacity is not None:
            token = self.capacity.try_acquire(inputs.route.provider, evaluation=True)
            if token is None:
                raise CheckpointConflict("Model capacity unavailable; defer without dispatch")
        try:
            reserved = (
                await self.runtime.reserve(intent.request, candidate)
                if previous is None
                else await self.runtime.replan(ReplanAuthorization(previous, candidate))
            )
            if not reserved.acquired or reserved.execution.plan.digest != candidate.digest:
                raise CandidateDispatchPending("Another candidate owns the first dispatch")
            _, operation, archive = read_candidate_dispatch_plan(
                intent, reserved.execution.plan, route_resolver=self.runtime.routing
            )
            business = CandidateDispatchWrite()
            root = EvaluationStateRoot(
                self.transactions,
                self.archives,
                self.runtime.store,
                intent,
                reserved.execution.plan,
                business,
                database_seconds=self.planner.database_seconds,
                isolation_level="REPEATABLE READ",
                route_resolver=self.runtime.routing,
            )
            try:
                result = await self.runtime.execute(reserved.execution, root)
            except TransactionSendError as error:
                result = error.result
                if result.local != LocalOutcome.COMMITTED:
                    raise
            if result.local != LocalOutcome.COMMITTED:
                raise CandidateDispatchPending("Original candidate root is not confirmed committed")
            prepared = business.prepared
            if (
                prepared is None
                or prepared.claim is None
                or prepared.claim.checkpoint
                != operation.checkpoint.mark_dispatching(
                    self.owner, operation.checkpoint.claimed_at
                )
                or prepared.state.version != version + 1
            ):
                raise CandidateDispatchPending("Fresh original candidate handoff unavailable")
            async with asyncio.timeout(self.planner.database_seconds):
                evidence = tuple(
                    [await self.runtime.store.evidence(r) for r in reserved.execution.references]
                )
                if decide_local(reserved.execution.references, evidence) != LocalOutcome.COMMITTED:
                    raise CandidateDispatchPending("Exact candidate dispatch witness unavailable")
                if await self.archives.read_committed(archive.envelope().message_id) != archive:
                    raise CandidateDispatchPending("Exact first candidate archive unavailable")
                async with self.transactions.open() as db:
                    current = await require_claim(db, prepared.claim, lock=False)
                if current.checkpoint.lease_expires_at <= self.planner.clock():
                    raise CandidateDispatchPending("Original candidate model permit expired")
            prepared = replace(
                prepared,
                claim=current,
                cp=current.checkpoint,
                state=replace(prepared.state, checkpoint=current.checkpoint),
                capacity_token=token,
            )
            token = None  # Original workflow's finally now owns this one capacity permit.
            return prepared
        finally:
            if token is not None:
                token.release()


class CandidateDispatchRecovery:
    """Explicit host owner for aborted pre-business dispatch; ordinary Sender stays strict."""

    def __init__(self, sender: CandidateDispatchSender) -> None:
        self.sender = sender

    async def __call__(self, run_id: UUID, version: int, organization_id: int) -> PreparedStep:
        intent = candidate_dispatch_intent(run_id, version, organization_id)
        async with asyncio.timeout(self.sender.planner.database_seconds):
            old = await self.sender.runtime.store.lookup(intent.request)
        if old is not None and old.state == State.ABORTED:
            return await self.sender.replan(run_id, version, organization_id, old.execution)
        return await self.sender(run_id, version, organization_id)
