"""Plan original unknown decisions and borrow the same guarded root and capacity policy."""

import asyncio
import math
from datetime import datetime
from typing import Any
from uuid import uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_capacity import (
    lock_admission,
    plan_admission,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import plan_resolution
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
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
from qs_ai.infrastructure.workflow_transport.resolution_state_plan import ResolutionStateOperation
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state
from qs_ai.infrastructure.workflow_transport.transaction_archive import FrozenArchive


def read_resolution_plan(
    operation: ResolutionStateOperation, winner: FrozenPlan
) -> tuple[EvaluationStateFacts, FrozenArchive]:
    facts, first = read_evaluation_state_plan(operation.intent, winner)
    ResolutionStateOperation.from_facts(facts)
    state = pb.MessagingBody.FromString(first.body).evaluation_state
    if (
        winner.operation_id != str(operation.operation_id)
        or state.version != operation.expected_version + 1
        or state.status not in ("collecting", "blocked", "canceled")
    ):
        raise MessageConflict("Exact original resolution target required")
    return facts, first


class ResolutionStatePlanner:
    def __init__(
        self,
        original: MySQLEvaluationManagement,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive resolution database budget required")
        self._original, self._database_seconds = original, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key

    async def candidate(self, operation: ResolutionStateOperation, at: datetime) -> FrozenPlan:
        EvaluationStateFacts(operation.intent, at, 0, 0)
        if operation.value.resolved_at != at:
            raise ValueError("Exact first manual resolution clock required")
        async with (
            asyncio.timeout(self._database_seconds),
            self._original.transactions.open() as db,
        ):
            run = await header(db, operation.scope)
            result = await plan_resolution(
                db,
                operation.scope.run_id,
                operation.expected_version,
                operation.scope.organization_id,
                operation.value,
                confirm=operation.confirm,
            )
            progress = result.progress()
            if progress["status"] == "collecting":
                await plan_admission(
                    db,
                    operation.scope,
                    self._original.capacity,
                    operation.value.resolved_at,
                    self._original.quota_baseline,
                )
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
                {**run, "version": result.state.version, "progress_json": progress},
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
        winner = freeze_evaluation_state_plan(facts, str(operation.operation_id), first)
        read_resolution_plan(operation, winner)
        return winner


class ResolutionStateRoot(EvaluationStateRoot):
    def __init__(
        self,
        original: MySQLEvaluationManagement,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        operation: ResolutionStateOperation,
        winner: FrozenPlan,
        *,
        database_seconds: float = 2,
    ) -> None:
        facts, _ = read_resolution_plan(operation, winner)
        frozen, digest = ResolutionStateOperation.from_facts(facts)
        self._view: EvaluationView | None = None
        self._business_error: Exception | None = None

        async def write(db: AsyncSession, bound: EvaluationStateFacts) -> None:
            try:
                if bound != facts:
                    raise MessageConflict("Original resolution facts changed")
                # Before any business snapshot/CP lock, as in the original resolver.
                await lock_admission(db, frozen.scope.organization_id)
                run = await header(db, frozen.scope, establish_snapshot=False)
                if run["version"] != frozen.expected_version or run["checkpoint_json"] is not None:
                    raise CheckpointConflict("Resolution version changed or execution is active")
                if source_snapshot_digest(run) != digest:
                    raise MessageConflict("Original unknown-resolution source changed")
                borrowed = MySQLEvaluationManagement(
                    Transactions.borrowed(db),
                    original.capacity,
                    original.models,
                    original.quota_baseline,
                    original.runtime_limits,
                )
                # Reuses the original mutex, CP/Run locks, decision, admission
                # and response. Borrowed commit never commits or closes this root.
                self._view = await borrowed.resolve(
                    frozen.scope,
                    frozen.expected_version,
                    frozen.value,
                    confirm=frozen.confirm,
                )
            except Exception as error:
                self._business_error = error
                raise

        super().__init__(
            original.transactions,
            archives,
            store,
            operation.intent,
            winner,
            write,
            database_seconds=database_seconds,
            isolation_level="REPEATABLE READ",
        )

    @property
    def view(self) -> EvaluationView | None:
        return self._view

    @property
    def business_error(self) -> Exception | None:
        return self._business_error
