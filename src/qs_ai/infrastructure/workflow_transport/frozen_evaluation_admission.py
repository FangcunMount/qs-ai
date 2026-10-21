"""Recheck frozen evaluation source under current locks, then borrow the original rules."""

from datetime import UTC, datetime

from reliable_messaging.transaction import BindingError, FrozenPlan
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow.command_mapping import evaluation_arguments
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_admission import evaluation_source_digest
from qs_ai.infrastructure.persistence.mysql.evaluation_capacity import lock_admission
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.workflow_transport.admission_receipts import AdmissionDecision
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.evaluation_plan import EvaluationPlanFacts
from qs_ai.infrastructure.workflow_transport.frozen_evaluation_events import FrozenEvaluationEvents
from qs_ai.infrastructure.workflow_transport.messaging import FIELDS
from qs_ai.infrastructure.workflow_transport.transaction_command import FrozenCommandInput


class FrozenEvaluationAdmission:
    def __init__(
        self, incoming: FrozenCommandInput, plan: FrozenPlan, admission: WorkflowCommandAdmission
    ) -> None:
        self._facts = EvaluationPlanFacts.from_plan(incoming, plan)
        self._incoming, self._plan_digest, self._base = incoming, plan.digest, admission
        self._admission = WorkflowCommandAdmission(
            admission.source,
            admission.participant_capacity,
            admission.evaluation_capacity,
            admission.models,
            admission.quota_baseline,
            admission.runtime_limits,
            evaluation_factory=self._store,
            clock=lambda: self._facts.at,
        )

    @property
    def facts(self) -> EvaluationPlanFacts:
        return self._facts

    def _store(self, transactions: Transactions) -> MySQLEvaluationManagement:
        base = self._base
        return MySQLEvaluationManagement(
            transactions,
            base.evaluation_capacity,
            base.models,
            base.quota_baseline,
            base.runtime_limits,
        )

    async def __call__(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, body: pb.MessagingBody
    ) -> AdmissionDecision:
        if envelope != self._incoming.envelope() or body != self._incoming.body_message():
            raise BindingError("Original authenticated evaluation differs")
        events = db.info.get("state_events")
        if (
            not isinstance(events, FrozenEvaluationEvents)
            or events.plan_digest != self._plan_digest
        ):
            raise BindingError("Exact original evaluation transcription required")
        try:
            scope, _, _, _ = evaluation_arguments(getattr(body, FIELDS[envelope.kind]))
        except ValueError:
            return await self._admission.admit(db, envelope, body)
        if envelope.kind == pb.EVALUATION_START:
            # Root opts into current reads before the guard's first SQL. This
            # mutex still precedes all evaluation locks/capacity reads, keeping
            # accepted starts on a single original organization budget.
            await lock_admission(db, scope.organization_id)
        checkpoint = (
            (
                await db.execute(
                    select(evaluation_checkpoints)
                    .where(evaluation_checkpoints.c.run_id == str(scope.run_id))
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        run = (
            (
                await db.execute(
                    select(evaluation_runs)
                    .where(
                        evaluation_runs.c.run_id == str(scope.run_id),
                        evaluation_runs.c.organization_id == scope.organization_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if self._facts.source_digest is not None:
            if checkpoint is None or run is None:
                raise ConfigurationPlanConflict("Frozen original evaluation source missing")
            current = {
                **run,
                "version": checkpoint["version"],
                "checkpoint_json": checkpoint["checkpoint_json"],
            }
            if evaluation_source_digest(current) != self._facts.source_digest:
                raise ConfigurationPlanConflict("Frozen original evaluation source changed")
        now = await db.scalar(select(func.utc_timestamp(6)))
        if not isinstance(now, datetime):
            raise ConfigurationPlanConflict("Authoritative evaluation root clock unavailable")
        self._facts.validate_clock(now.replace(tzinfo=UTC) if now.tzinfo is None else now)
        return await self._admission.admit(db, envelope, body)
