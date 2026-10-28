"""Finalize an already durable serial response; never grant provider invocation authority."""

import asyncio

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    LocalOutcome,
    ReplanAuthorization,
    RetryAuthorization,
    State,
    decide_local,
)

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.completion_records import semantic_completions
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_semantic_completion
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    SerialResponseReference,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.serial_semantic_state import (
    SerialSemanticPlanner,
    SerialSemanticWrite,
    completion_digest,
    read_serial_semantic_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class SerialSemanticPending(RuntimeError):
    """Resolve the original terminal responsibility without another provider invocation."""


class SerialSemanticSender:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        planner: SerialSemanticPlanner,
    ) -> None:
        if planner.transactions is not transactions:
            raise ValueError("Original serial completion transaction owner must match")
        self._transactions, self._archives, self._runtime, self._planner = (
            transactions,
            archives,
            runtime,
            planner,
        )

    async def _committed(
        self, intent: EvaluationStateIntent, execution: Execution
    ) -> CheckpointState:
        _, operation = read_serial_semantic_plan(
            intent, execution.plan, route_resolver=self._runtime.routing
        )
        _, first = read_evaluation_state_plan(
            intent, execution.plan, route_resolver=self._runtime.routing
        )
        ref = operation.reference
        async with asyncio.timeout(self._planner.database_seconds):
            evidence = tuple([await self._runtime.store.evidence(r) for r in execution.references])
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise SerialSemanticPending("Exact original serial completion witness unavailable")
            if await self._archives.read_committed(first.envelope().message_id) != first:
                raise SerialSemanticPending("Exact first serial completion archive unavailable")
            async with self._transactions.open() as db:
                stored = (
                    (
                        await db.execute(
                            semantic_completions.select()
                            .join(
                                evaluation_runs,
                                evaluation_runs.c.run_id == semantic_completions.c.run_id,
                            )
                            .where(
                                semantic_completions.c.run_id == str(ref.run_id),
                                semantic_completions.c.execution_id == ref.checkpoint.execution_id,
                                evaluation_runs.c.organization_id == ref.organization_id,
                            )
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    stored is None
                    or stored["candidate_id"] != (operation.candidate_id or None)
                    or completion_digest(decode_semantic_completion(stored))
                    != operation.completion_hash
                ):
                    raise SerialSemanticPending(
                        "Exact original terminal business receipt unavailable"
                    )
        # Return the first result even after the Run has legitimately advanced again.
        state = pb.MessagingBody.FromString(first.body).evaluation_state
        return CheckpointState(ref.run_id, state.version, None)

    async def __call__(self, reference: SerialResponseReference) -> CheckpointState:
        intent = await self._planner.intent(reference)
        async with asyncio.timeout(self._planner.database_seconds):
            reservation = await self._runtime.store.lookup(intent.request)
        if reservation is None:
            winner = await self._planner.candidate(reference)
            reservation = await self._runtime.reserve(intent.request, winner)
        if not reservation.acquired:
            if reservation.state == State.COMMITTED:
                return await self._committed(intent, reservation.execution)
            raise SerialSemanticPending("Original serial completion is already owned or unresolved")
        return await self._execute_owned(intent, reservation.execution)

    async def retry(
        self, reference: SerialResponseReference, previous: Execution
    ) -> CheckpointState:
        """Explicit host finalization decision after exact abort; no automatic/model retry."""
        intent = await self._planner.intent(reference)
        await self._planner.verify(reference, previous.plan)
        reservation = await self._runtime.retry(RetryAuthorization(previous))
        if not reservation.acquired:
            raise SerialSemanticPending("Explicit serial completion retry was not acquired")
        return await self._execute_owned(intent, reservation.execution)

    async def replan(
        self, reference: SerialResponseReference, previous: Execution
    ) -> CheckpointState:
        """Host-authorized new source plan after an exact old abort; preserve first candidate."""
        intent = await self._planner.intent(reference)
        _, first = read_serial_semantic_plan(
            intent, previous.plan, route_resolver=self._runtime.routing
        )
        candidate = await self._planner.candidate(reference, candidate_id=first.candidate_id)
        reservation = await self._runtime.replan(ReplanAuthorization(previous, candidate))
        if not reservation.acquired:
            raise SerialSemanticPending("Explicit serial completion replan was not acquired")
        return await self._execute_owned(intent, reservation.execution)

    async def _execute_owned(
        self, intent: EvaluationStateIntent, execution: Execution
    ) -> CheckpointState:
        # Do not let a public runtime substitute a plan bound to another original input.
        read_serial_semantic_plan(intent, execution.plan, route_resolver=self._runtime.routing)
        business = SerialSemanticWrite()
        root = EvaluationStateRoot(
            self._transactions,
            self._archives,
            self._runtime.store,
            intent,
            execution.plan,
            business,
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
            raise SerialSemanticPending("Original serial completion root not confirmed committed")
        return await self._committed(intent, execution)
