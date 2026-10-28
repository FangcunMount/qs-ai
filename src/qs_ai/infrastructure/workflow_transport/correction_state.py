"""Original correction rules, exact historical replay and one guarded state root."""

import asyncio
import math
from datetime import datetime
from typing import Any
from uuid import uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from reliable_messaging.transport import RouteResolver
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.execution_mode import (
    SERIAL_RUNTIME_LIMITS,
    EvaluationRuntimeLimits,
)
from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import (
    lock_run_in_transaction,
    save_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
from qs_ai.infrastructure.persistence.mysql.evaluation_review_corrections import (
    correction_replayed,
    plan_review_correction,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    source_snapshot_digest,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive


def arguments(operation: CorrectionStateOperation) -> dict[str, Any]:
    return dict(
        scope=operation.scope,
        expected_version=operation.expected_version,
        command_id=operation.command_id,
        previous_review_fingerprint=operation.previous_review_fingerprint,
        candidate_output_fingerprint=operation.candidate_output_fingerprint,
        value=operation.value,
    )


def read_correction_plan(
    operation: CorrectionStateOperation,
    winner: FrozenPlan,
    *,
    route_resolver: RouteResolver,
) -> tuple[EvaluationStateFacts, FrozenArchive]:
    facts, archived = read_evaluation_state_plan(
        operation.intent, winner, route_resolver=route_resolver
    )
    CorrectionStateOperation.from_facts(facts)
    state = pb.MessagingBody.FromString(archived.body).evaluation_state
    if (
        winner.operation_id != operation.command_id
        or state.version != operation.expected_version + 1
        or state.status != "awaiting_review"
    ):
        raise MessageConflict("Exact original correction target required")
    return facts, archived


class CorrectionStatePlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive correction database budget required")
        self._transactions, self._database_seconds = transactions, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key
        self._runtime_limits = runtime_limits

    async def replay(self, operation: CorrectionStateOperation) -> EvaluationView | None:
        async with asyncio.timeout(self._database_seconds), self._transactions.open() as db:
            run = await header(db, operation.scope)
            if run["checkpoint_json"] is not None:
                raise CheckpointConflict("Correction execution is active")
            if correction_replayed(**arguments(operation), progress=run["progress_json"]):
                return await read_view(db, operation.scope, self._runtime_limits)
        return None

    async def candidate(
        self, operation: CorrectionStateOperation, at: datetime
    ) -> FrozenPlan | None:
        EvaluationStateFacts(operation.intent, at, 0, 0)
        async with asyncio.timeout(self._database_seconds), self._transactions.open() as db:
            run = await header(db, operation.scope)
            if run["checkpoint_json"] is not None:
                raise CheckpointConflict("Correction execution is active")
            progress = await plan_review_correction(db, **arguments(operation), run=run)
            if progress is None:
                # Another original writer committed the same business command.
                # The caller must re-read its receipt, without preparing an event.
                return None
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(operation.scope.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            facts = EvaluationStateFacts(
                operation.intent,
                at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                operation.encode(source_snapshot_digest(run)),
            )
            visible = evaluation_state(
                str(operation.scope.run_id),
                {**run, "version": operation.expected_version + 1, "progress_json": progress},
                facts.sequence,
            )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(operation.scope.run_id, f"state:{visible.version}")),
            str(operation.scope.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(operation.scope.organization_id),
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            original_occurred_at=at.isoformat(timespec="microseconds"),
        )
        return freeze_evaluation_state_plan(
            facts, operation.command_id, first, route_resolver=self._route_resolver
        )


async def apply_correction_state(db: AsyncSession, facts: EvaluationStateFacts) -> None:
    operation, digest = CorrectionStateOperation.from_facts(facts)
    run = await lock_run_in_transaction(db, operation.scope, operation.expected_version)
    if source_snapshot_digest(run) != digest:
        raise MessageConflict("Original correction source changed")
    progress = await plan_review_correction(db, **arguments(operation), run=run)
    if progress is None:
        raise MessageConflict("Historical correction cannot adopt a new transaction witness")
    await save_progress(db, operation.scope, operation.expected_version, progress)


class CorrectionStateRoot(EvaluationStateRoot):
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        operation: CorrectionStateOperation,
        winner: FrozenPlan,
        *,
        database_seconds: float = 2,
        runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        facts, _ = read_correction_plan(operation, winner, route_resolver=route_resolver)
        original, _ = CorrectionStateOperation.from_facts(facts)
        self._view: EvaluationView | None = None
        self._business_error: Exception | None = None

        async def write(db: AsyncSession, bound: EvaluationStateFacts) -> None:
            try:
                await apply_correction_state(db, bound)
                self._view = await read_view(db, original.scope, runtime_limits)
            except Exception as error:
                self._business_error = error
                raise

        super().__init__(
            transactions,
            archives,
            store,
            operation.intent,
            winner,
            write,
            database_seconds=database_seconds,
            isolation_level="REPEATABLE READ",
            route_resolver=route_resolver,
        )

    @property
    def view(self) -> EvaluationView | None:
        return self._view

    @property
    def business_error(self) -> Exception | None:
        return self._business_error
