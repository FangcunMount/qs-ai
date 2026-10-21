"""Original answer/cancel action using the durable winner, no external client."""

from datetime import UTC, datetime

from reliable_messaging.transaction import BindingError, FrozenPlan
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.ports import AccessDenied, NotFound
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow.command_mapping import change_arguments
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.persistence.mysql.interpretation import (
    MySQLUnitOfWorkFactory,
    session_from,
)
from qs_ai.infrastructure.persistence.mysql.schema import sessions
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    refusal_decision,
)
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.frozen_authorization import FrozenEvidenceSource
from qs_ai.infrastructure.workflow_transport.frozen_start_events import FrozenInterpretationEvents
from qs_ai.infrastructure.workflow_transport.interpretation_plan import InterpretationMutationFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import FrozenCommandInput


class FrozenChangeAdmission:
    def __init__(
        self, incoming: FrozenCommandInput, plan: FrozenPlan, admission: WorkflowCommandAdmission
    ) -> None:
        if incoming.envelope().kind != pb.CHANGE:
            raise BindingError("Only original CHANGE action supported")
        self._incoming = incoming
        self._facts = InterpretationMutationFacts.from_plan(incoming, plan)
        self._plan_digest, self._base = plan.digest, admission
        self._source = FrozenEvidenceSource(self._facts.authorization)
        self._admission = WorkflowCommandAdmission(
            self._source,
            admission.participant_capacity,
            admission.evaluation_capacity,
            admission.models,
            admission.quota_baseline,
            admission.runtime_limits,
            interpretation_factory=self._service,
        )

    @property
    def facts(self) -> InterpretationMutationFacts:
        return self._facts

    def _service(self, transactions: Transactions) -> InterpretationService:
        facts, base = self._facts, self._base
        return InterpretationService(
            MySQLUnitOfWorkFactory(
                transactions,
                base.participant_capacity,
                base.quota_baseline,
                base.models,
                job_id=facts.job_id,
                state_event_id=facts.state_event_id,
                at=facts.at,
            ),
            self._source,
            new_identity=facts.identity,
        )

    async def __call__(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, body: pb.MessagingBody
    ) -> AdmissionDecision:
        if envelope != self._incoming.envelope() or body != self._incoming.body_message():
            raise BindingError("Original authenticated CHANGE input differs")
        events = db.info.get("state_events")
        if (
            not isinstance(events, FrozenInterpretationEvents)
            or events.plan_digest != self._plan_digest
        ):
            raise BindingError("Exact original CHANGE transcription required")
        try:
            actor, session_id, _, _ = change_arguments(body.change)
        except ValueError:
            # Original admission records its deterministic malformed-command refusal.
            return await self._admission.admit(db, envelope, body)
        # This locking/current read precedes the original ownership/read projection.
        # It prevents a former authorization snapshot from following a changed
        # tenant, owner or assessment target in a repeatable-read root.
        row = (
            (
                await db.execute(
                    select(sessions).where(sessions.c.id == session_id).with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return refusal_decision(pb.CHANGE, self._incoming.organization_id, NotFound())
        current = session_from(row)
        if current.actor != actor:
            return refusal_decision(pb.CHANGE, self._incoming.organization_id, AccessDenied())
        now = await db.scalar(select(func.utc_timestamp(6)))
        if not isinstance(now, datetime):
            raise ConfigurationPlanConflict("Authoritative CHANGE clock unavailable")
        self._facts.validate_observation(now.replace(tzinfo=UTC) if now.tzinfo is None else now)
        if not current.uses_qs_snapshot:
            try:
                await self._source.authorize(actor, current.testee_id, current.assessment_ids)
            except AccessDenied:
                # Keep the original refusal/savepoint mapping, not a root abort.
                pass
        return await self._admission.admit(db, envelope, body)
