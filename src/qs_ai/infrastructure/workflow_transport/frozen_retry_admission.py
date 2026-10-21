"""Execute only the original explicitly authorized retry in the guarded root."""

from datetime import UTC, datetime

from reliable_messaging.transaction import BindingError, FrozenPlan
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.ports import AccessDenied, NotFound
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow.command_mapping import participant_retry_command
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.persistence.mysql.interpretation import session_from
from qs_ai.infrastructure.persistence.mysql.retry_admission import FrozenParticipantRetries
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


class FrozenRetryAdmission:
    def __init__(
        self, incoming: FrozenCommandInput, plan: FrozenPlan, admission: WorkflowCommandAdmission
    ) -> None:
        if incoming.envelope().kind != pb.PARTICIPANT_RETRY:
            raise BindingError("Only original participant retry action supported")
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
            retry_factory=self._store,
        )

    @property
    def facts(self) -> InterpretationMutationFacts:
        return self._facts

    def _store(self, transactions: Transactions) -> FrozenParticipantRetries:
        facts, base = self._facts, self._base
        return FrozenParticipantRetries(
            transactions,
            base.participant_capacity,
            base.quota_baseline,
            at=facts.at,
            run_id=facts.run_id,
            job_id=facts.job_id,
            state_event_id=facts.state_event_id,
            expected_audit=facts.audit_digest,
        )

    async def __call__(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, body: pb.MessagingBody
    ) -> AdmissionDecision:
        if envelope != self._incoming.envelope() or body != self._incoming.body_message():
            raise BindingError("Original authenticated participant retry differs")
        events = db.info.get("state_events")
        if (
            not isinstance(events, FrozenInterpretationEvents)
            or events.plan_digest != self._plan_digest
        ):
            raise BindingError("Exact original retry transcription required")
        try:
            command = participant_retry_command(body.participant_retry)
        except ValueError:
            return await self._admission.admit(db, envelope, body)
        row = (
            (
                await db.execute(
                    select(sessions)
                    .where(
                        sessions.c.id == command.session_id,
                        sessions.c.org_id == command.scope.organization_id,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return refusal_decision(
                pb.PARTICIPANT_RETRY, self._incoming.organization_id, NotFound()
            )
        current = session_from(row)
        if not current.uses_qs_snapshot:
            return refusal_decision(
                pb.PARTICIPANT_RETRY,
                self._incoming.organization_id,
                RuleViolation("participant_retry_requires_snapshot"),
            )
        now = await db.scalar(select(func.utc_timestamp(6)))
        if not isinstance(now, datetime):
            raise ConfigurationPlanConflict("Authoritative retry root clock unavailable")
        self._facts.validate_observation(now.replace(tzinfo=UTC) if now.tzinfo is None else now)
        try:
            # QS delegated the operator. The frozen observation additionally
            # binds the current original participant, not that operator's ID.
            await self._source.authorize(current.actor, current.testee_id, current.assessment_ids)
        except AccessDenied:
            pass  # Original service/savepoint maps the exact known refusal.
        return await self._admission.admit(db, envelope, body)
