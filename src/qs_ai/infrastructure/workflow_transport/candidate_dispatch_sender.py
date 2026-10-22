"""Only a fresh local candidate dispatch can transfer the original model-capacity permit."""

import asyncio
from dataclasses import replace
from uuid import UUID

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import LocalOutcome, decide_local

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
from qs_ai.infrastructure.workflow_transport.transaction_root import StateTransactions


class CandidateDispatchPending(RuntimeError):
    """No provider call is authorized while the first dispatch responsibility is unresolved."""


class CandidateDispatchSender:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: StateTransactions,
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
        intent = candidate_dispatch_intent(run_id, version, organization_id)
        async with asyncio.timeout(self.planner.database_seconds):
            existing = await self.runtime.store.lookup(intent.request)
        if existing is not None:
            # Even a committed dispatch is not permission to invoke the provider twice.
            raise CandidateDispatchPending("Existing candidate dispatch requires exact recovery")
        candidate, inputs = await self.planner.candidate(
            run_id, version, organization_id, self.owner
        )
        token = None
        if self.capacity is not None:
            token = self.capacity.try_acquire(inputs.route.provider, evaluation=True)
            if token is None:
                raise CheckpointConflict("Model capacity unavailable; defer without dispatch")
        try:
            reserved = await self.runtime.reserve(intent.request, candidate)
            if not reserved.acquired or reserved.execution.plan.digest != candidate.digest:
                raise CandidateDispatchPending("Another candidate owns the first dispatch")
            _, operation, archive = read_candidate_dispatch_plan(intent, reserved.execution.plan)
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
