"""Freeze review state before halves, then apply original rules in the guarded root."""

import asyncio
import math
from dataclasses import replace
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
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import lock_run_in_transaction
from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
from qs_ai.infrastructure.persistence.mysql.evaluation_reviews import accept_reviews, plan_reviews
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_governance_state import (
    source_snapshot_digest,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.review_state_plan import (
    ReviewPlanFacts,
    ReviewStateInput,
    read_review_state_plan,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state


class ReviewStatePlanner:
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
            raise ValueError("Finite positive review database budget required")
        self._transactions, self._database_seconds = transactions, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key
        self._runtime_limits = runtime_limits

    async def candidate(self, incoming: ReviewStateInput, at: datetime) -> FrozenPlan:
        EvaluationStateFacts(incoming.intent, at, 0, 0)
        async with asyncio.timeout(self._database_seconds), self._transactions.open() as db:
            run = await header(db, incoming.scope)
            if run["version"] != incoming.expected_version or run["checkpoint_json"] is not None:
                raise CheckpointConflict("Review version changed or execution is active")
            # Preserve the original management adapter's pre-write audit checks.
            await read_view(db, incoming.scope, self._runtime_limits)
            state, progress = await plan_reviews(
                db, incoming.scope, incoming.expected_version, incoming.values, lock=False
            )
            previous = (
                (
                    await db.execute(
                        select(evaluation_sequences).where(
                            evaluation_sequences.c.run_id == str(incoming.scope.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            original = ReviewPlanFacts(
                source_snapshot_digest(run),
                incoming.input_digest,
                tuple(v.reviewed_at for v in incoming.values),
            )
            facts = EvaluationStateFacts(
                incoming.intent,
                at,
                previous["sequence"] if previous else 0,
                previous["version"] if previous else 0,
                original.encode(),
            )
            visible = evaluation_state(
                str(incoming.scope.run_id),
                {**run, "version": state.version, "progress_json": progress},
                facts.sequence,
            )
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(incoming.scope.run_id, f"state:{visible.version}")),
            str(incoming.scope.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(incoming.scope.organization_id),
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            original_occurred_at=at.isoformat(timespec="microseconds"),
        )
        return freeze_evaluation_state_plan(
            facts, str(incoming.operation_id), first, route_resolver=self._route_resolver
        )


async def apply_review_state(
    db: AsyncSession,
    incoming: ReviewStateInput,
    facts: EvaluationStateFacts,
    original: ReviewPlanFacts,
    runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
) -> None:
    """Use the already restored first input; caller owns isolation, guard and commit."""
    if (
        incoming.intent != facts.intent
        or incoming.input_digest != original.input_digest
        or tuple(v.reviewed_at.isoformat() for v in incoming.values)
        != tuple(at.isoformat() for at in original.reviewed_at)
    ):
        raise MessageConflict("Original frozen review input required")
    run = await lock_run_in_transaction(db, incoming.scope, incoming.expected_version)
    if source_snapshot_digest(run) != original.snapshot_digest:
        raise MessageConflict("Original review source snapshot changed")
    await read_view(db, incoming.scope, runtime_limits)
    await accept_reviews(db, incoming.scope, incoming.expected_version, incoming.values)


class ReviewStateRoot(EvaluationStateRoot):
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        incoming: ReviewStateInput,
        winner: FrozenPlan,
        *,
        database_seconds: float = 2,
        runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        facts, original, _ = read_review_state_plan(incoming, winner, route_resolver=route_resolver)
        # Restore the durable winner's first clocks before entering the root.
        frozen = replace(incoming, values=original.values_for(incoming))
        self._view: EvaluationView | None = None
        self._business_error: Exception | None = None

        async def write(db: AsyncSession, bound: EvaluationStateFacts) -> None:
            try:
                await apply_review_state(db, frozen, bound, original, runtime_limits)
                self._view = await read_view(db, frozen.scope, runtime_limits)
            except Exception as error:
                self._business_error = error
                raise

        super().__init__(
            transactions,
            archives,
            store,
            facts.intent,
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
