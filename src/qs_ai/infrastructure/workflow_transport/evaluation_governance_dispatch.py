"""One authorized governance invocation; no new cross-RPC retry contract."""

import asyncio
import math
from datetime import datetime
from typing import Any

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    BindingError,
    Execution,
    FrozenPlan,
    LocalOutcome,
    Reservation,
    SchemaError,
    State,
    decide_local,
)

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.execution_mode import (
    SERIAL_RUNTIME_LIMITS,
    EvaluationRuntimeLimits,
)
from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_input import input_conflicts
from qs_ai.infrastructure.workflow_transport.correction_state import (
    CorrectionStatePlanner,
    CorrectionStateRoot,
    read_correction_plan,
)
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    GovernanceStateOperation,
    GovernanceStatePlanner,
    GovernanceStateRoot,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.resolution_state import (
    ResolutionStatePlanner,
    ResolutionStateRoot,
)
from qs_ai.infrastructure.workflow_transport.resolution_state_plan import ResolutionStateOperation
from qs_ai.infrastructure.workflow_transport.review_state import ReviewStatePlanner, ReviewStateRoot
from qs_ai.infrastructure.workflow_transport.review_state_plan import ReviewStateInput
from qs_ai.infrastructure.workflow_transport.transaction_root import StateTransactions

StateOperation = (
    GovernanceStateOperation
    | ReviewStateInput
    | CorrectionStateOperation
    | ResolutionStateOperation
)


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
        resolution_management: MySQLEvaluationManagement | None = None,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive governance evidence budget required")
        if (
            resolution_management is not None
            and resolution_management.transactions is not transactions
        ):
            raise ValueError("Resolution must borrow this exact host transaction owner")
        self._transactions, self._archives, self._runtime = transactions, archives, runtime
        self._database_seconds, self._runtime_limits = database_seconds, runtime_limits
        self._resolution_original = resolution_management
        self._resolution_planner = (
            ResolutionStatePlanner(
                resolution_management,
                signing_key=signing_key,
                recipient_key=recipient_key,
                database_seconds=database_seconds,
                route_resolver=runtime.routing,
            )
            if resolution_management is not None
            else None
        )
        self._planner = GovernanceStatePlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            route_resolver=runtime.routing,
        )
        self._review_planner = ReviewStatePlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            runtime_limits=runtime_limits,
            route_resolver=runtime.routing,
        )

        self._correction_planner = CorrectionStatePlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            runtime_limits=runtime_limits,
            route_resolver=runtime.routing,
        )

    async def __call__(self, operation: StateOperation, at: datetime) -> EvaluationView:
        if isinstance(operation, ResolutionStateOperation) and self._resolution_planner is None:
            raise BindingError("Explicit original resolution management binding required")
        existing = await self._lookup(operation)
        if isinstance(operation, CorrectionStateOperation):
            replay = await self._correction_result(operation, existing)
            if replay is not None:
                return replay
        elif existing is not None:
            # This interface has no durable public operation-result reader.
            # Returning today's Run view would substitute a different response.
            raise GovernanceTransactionPending("Original governance invocation already exists")
        candidate: FrozenPlan | None
        if isinstance(operation, ResolutionStateOperation):
            assert self._resolution_planner is not None
            candidate = await self._resolution_planner.candidate(operation, at)
        elif isinstance(operation, CorrectionStateOperation):
            candidate = await self._correction_planner.candidate(operation, at)
            if candidate is None:
                replay = await self._correction_result(operation, await self._lookup(operation))
                if replay is not None:
                    return replay
                raise GovernanceTransactionPending("Original correction receipt unavailable")
        elif isinstance(operation, ReviewStateInput):
            candidate = await self._review_planner.candidate(operation, at)
        else:
            candidate = await self._planner.candidate(operation, at)
        try:
            reservation = await self._runtime.reserve(operation.intent.request, candidate)
        except (BindingError, SchemaError):
            raise
        except ValueError:
            await self._reject_changed_input(operation)
            raise
        if not reservation.acquired:
            if isinstance(operation, CorrectionStateOperation):
                replay = await self._correction_result(operation, reservation)
                if replay is not None:
                    return replay
            raise GovernanceTransactionPending("Original governance invocation is already owned")
        local: CorrectionStateRoot | ReviewStateRoot | GovernanceStateRoot | ResolutionStateRoot
        if isinstance(operation, ResolutionStateOperation):
            assert self._resolution_original is not None
            local = ResolutionStateRoot(
                self._resolution_original,
                self._archives,
                self._runtime.store,
                operation,
                reservation.execution.plan,
                database_seconds=self._database_seconds,
                route_resolver=self._runtime.routing,
            )
        elif isinstance(operation, CorrectionStateOperation):
            local = CorrectionStateRoot(
                self._transactions,
                self._archives,
                self._runtime.store,
                operation,
                reservation.execution.plan,
                database_seconds=self._database_seconds,
                runtime_limits=self._runtime_limits,
                route_resolver=self._runtime.routing,
            )
        elif isinstance(operation, ReviewStateInput):
            local = ReviewStateRoot(
                self._transactions,
                self._archives,
                self._runtime.store,
                operation,
                reservation.execution.plan,
                database_seconds=self._database_seconds,
                runtime_limits=self._runtime_limits,
                route_resolver=self._runtime.routing,
            )
        else:
            local = GovernanceStateRoot(
                self._transactions,
                self._archives,
                self._runtime.store,
                operation,
                reservation.execution.plan,
                database_seconds=self._database_seconds,
                runtime_limits=self._runtime_limits,
                route_resolver=self._runtime.routing,
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
        await self._verify_commit(operation.intent, reservation.execution)
        if local.view is None:
            raise GovernanceTransactionPending("Original governance response unavailable")
        # FinalUnknown remains unresolved Broker responsibility. Never rerun the
        # root or read a later Run to manufacture this invocation's response.
        return local.view

    async def _lookup(self, operation: StateOperation) -> Reservation | None:
        try:
            async with asyncio.timeout(self._database_seconds):
                return await self._runtime.store.lookup(operation.intent.request)
        except (BindingError, SchemaError):
            raise
        except ValueError:
            await self._reject_changed_input(operation)
            raise

    async def _reject_changed_input(self, operation: StateOperation) -> None:
        if isinstance(operation, CorrectionStateOperation):
            async with asyncio.timeout(self._database_seconds):
                conflict = await input_conflicts(
                    self._transactions, self._runtime.store.binding, operation.intent.request
                )
            if conflict:
                raise CheckpointConflict(
                    "Correction command ID reused with different intent"
                ) from None

    async def _correction_result(
        self, operation: CorrectionStateOperation, existing: Reservation | None
    ) -> EvaluationView | None:
        replay = None
        if existing is None:
            replay = await self._correction_planner.replay(operation)
            if replay is None:
                return None
            # Recheck in a new boundary: another SDK writer may have reserved
            # between the first lookup and this exact business receipt read.
            existing = await self._lookup(operation)
            if existing is None:
                return replay  # Historical legacy responsibility remains legacy.
        if existing.state != State.COMMITTED:
            raise GovernanceTransactionPending("Original correction execution is unresolved")
        read_correction_plan(
            operation, existing.execution.plan, route_resolver=self._runtime.routing
        )
        await self._verify_commit(operation.intent, existing.execution)
        if replay is None:
            replay = await self._correction_planner.replay(operation)
        if replay is None:
            raise GovernanceTransactionPending("Original correction business receipt unavailable")
        return replay

    async def _verify_commit(self, intent: EvaluationStateIntent, execution: Execution) -> None:
        _, value = read_evaluation_state_plan(
            intent, execution.plan, route_resolver=self._runtime.routing
        )
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise GovernanceTransactionPending("Exact original governance witness unavailable")
            if await self._archives.read_committed(value.envelope().message_id) != value:
                raise GovernanceTransactionPending("Exact original governance archive unavailable")
