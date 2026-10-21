"""Build a complete immutable START candidate before half messages or business writes.

The host first looks up the authoritative SDK request. After Reserve it must
read the durable winner, never keep using this candidate's local resources.
"""

from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import select

from qs_ai.application.execution.capacity import (
    DEFAULT_PARTICIPANT_CAPACITY,
    ParticipantCapacityPolicy,
)
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import (
    DEFAULT_EDITABLE_MODELS,
    EditableModelPolicy,
)
from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.contracts.workflow.command_mapping import start_arguments
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.persistence.mysql.start_admission import ReadOnlyStartUnitOfWork
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    build_receipt_body,
    refusal_decision,
    start_decision,
)
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    freeze_admission_plan,
)


def _clock() -> datetime:
    return datetime.now(UTC)


def _identity() -> str:
    return str(uuid4())


class StartAdmissionPlanner:
    def __init__(
        self,
        transactions: Transactions,
        source: EvidenceSource,
        *,
        signing_key: Any,
        recipient_key: Any,
        capacity: ParticipantCapacityPolicy = DEFAULT_PARTICIPANT_CAPACITY,
        quota_baseline: QuotaBaseline | None = None,
        models: EditableModelPolicy = DEFAULT_EDITABLE_MODELS,
        clock: Callable[[], datetime] | None = None,
        new_id: Callable[[], str] | None = None,
    ) -> None:
        self._transactions, self._source = transactions, source
        self._signing_key, self._recipient_key = signing_key, recipient_key
        self._capacity, self._quota_baseline, self._models = capacity, quota_baseline, models
        self._clock = _clock if clock is None else clock
        self._new_id = _identity if new_id is None else new_id

    async def candidate(self, incoming: FrozenCommandInput) -> FrozenPlan:
        envelope, body = incoming.envelope(), incoming.body_message()
        if envelope.kind != pb.START:
            raise ValueError("Only authenticated original START planning supported")
        operation_id, receipt_id = self._new_id(), self._new_id()
        facts = StartPlanFacts(
            incoming.request.input_hash,
            self._clock(),
            session_id=self._new_id(),
            run_id=self._new_id(),
            evidence_id=self._new_id(),
            job_id=self._new_id(),
            reservation_token=self._new_id(),
            state_event_id=self._new_id(),
        )
        state = None
        async with self._transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            uow = ReadOnlyStartUnitOfWork(
                db, facts, self._capacity, self._quota_baseline, self._models
            )
            service = InterpretationService(uow, self._source, new_identity=facts.identity)
            try:
                result = await service.start_external(*start_arguments(body.start))
            except (RuleViolation, ValueError) as error:
                # Same mapping as original admission/savepoint rollback. No staged
                # read projection becomes a business fact or an extra state event.
                decision = refusal_decision(pb.START, incoming.organization_id, error)
            else:
                state = uow.state
                event: object = asdict(state) if state is not None else None
                if result.status == "blocked" and state is None:
                    event = await db.scalar(
                        select(result_outbox.c.payload).where(
                            result_outbox.c.session_id == result.session_id,
                            result_outbox.c.version == result.version,
                        )
                    )
                decision = start_decision(body.start, result, incoming.organization_id, event)
                if uow.previous:
                    if not result.run_id:
                        raise RuntimeError("Original START Run identity unavailable")
                    facts = replace(facts, session_id=result.session_id, run_id=result.run_id)
                else:
                    facts = replace(facts, configuration=uow.configuration)
        # Read session/transaction cleanup precedes signing and encryption.
        at = facts.at.isoformat(timespec="microseconds")
        receipt = prepare(
            pb.COMMAND_RECEIPT,
            receipt_id,
            envelope.aggregate_key,
            build_receipt_body(envelope, decision),
            organization_id=incoming.organization_id,
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            correlation=envelope.message_id,
            original_occurred_at=at,
        )
        publications = [(receipt, decision.sequence)]
        if state is not None:
            prepared_state = prepare(
                pb.INTERPRETATION_STATE,
                state.event_id,
                envelope.aggregate_key,
                pb.MessagingBody(interpretation_state=workflow.StateEvent(**asdict(state))),
                organization_id=incoming.organization_id,
                signing_key=self._signing_key,
                recipient_key=self._recipient_key,
                original_occurred_at=at,
            )
            publications.append((prepared_state, state.version))
        plan = freeze_admission_plan(
            incoming, operation_id, tuple(publications), context=facts.encode()
        )
        StartPlanFacts.from_plan(incoming, plan)
        return plan
