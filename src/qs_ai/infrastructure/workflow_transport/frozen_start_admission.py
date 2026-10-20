"""Execute original START admission with the durable winner's frozen resources.

This action owns neither a connection nor the root commit. It is invoked only
inside AdmissionRoot after the SDK execution guard and Inbox reservation.
"""

from reliable_messaging.transaction import BindingError, FrozenPlan
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.workflow_transport.admission_receipts import AdmissionDecision
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.frozen_start_events import FrozenStartEvents
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import FrozenCommandInput


class FrozenStartAdmission:
    def __init__(
        self,
        incoming: FrozenCommandInput,
        plan: FrozenPlan,
        admission: WorkflowCommandAdmission,
    ) -> None:
        self._incoming = incoming
        self._facts = StartPlanFacts.from_plan(incoming, plan)
        self._plan_digest = plan.digest
        self._base = admission
        self._admission = WorkflowCommandAdmission(
            admission.source,
            admission.participant_capacity,
            admission.evaluation_capacity,
            admission.models,
            admission.quota_baseline,
            admission.runtime_limits,
            interpretation_factory=self._service,
        )

    @property
    def facts(self) -> StartPlanFacts:
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
                configuration=facts.configuration,
            ),
            base.source,
            new_identity=facts.identity,
        )

    async def __call__(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, body: pb.MessagingBody
    ) -> AdmissionDecision:
        if envelope != self._incoming.envelope() or body != self._incoming.body_message():
            raise BindingError("Original authenticated START input differs")
        events = db.info.get("state_events")
        if not isinstance(events, FrozenStartEvents) or events.plan_digest != self._plan_digest:
            raise BindingError("Exact original START transcription required")
        # A request crossing UTC midnight must be explicitly replanned. A frozen
        # former budget day cannot reserve from yesterday's quota in the root.
        day = await db.scalar(select(func.utc_date()))
        if day != self._facts.at.date():
            raise ConfigurationPlanConflict("Frozen START budget day changed")
        return await self._admission.admit(db, envelope, body)
