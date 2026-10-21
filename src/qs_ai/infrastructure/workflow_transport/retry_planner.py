"""Freeze original explicitly authorized participant retry before any half message."""

from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from reliable_messaging.transaction import FrozenPlan

from qs_ai.application.execution.capacity import (
    DEFAULT_PARTICIPANT_CAPACITY,
    ParticipantCapacityPolicy,
)
from qs_ai.application.execution.retry import RetryParticipant
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.interpretation.ports import AccessDenied, EvidenceSource, NotFound
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.contracts.workflow.command_mapping import participant_retry_command, receipt_message
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.retry_admission import ReadOnlyParticipantRetries
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    build_receipt_body,
    refusal_decision,
)
from qs_ai.infrastructure.workflow_transport.frozen_authorization import RecordingEvidenceSource
from qs_ai.infrastructure.workflow_transport.interpretation_plan import InterpretationMutationFacts
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    freeze_admission_plan,
)


def _clock() -> datetime:
    return datetime.now(UTC)


def _identity() -> str:
    return str(uuid4())


class ParticipantRetryPlanner:
    def __init__(
        self,
        transactions: Transactions,
        source: EvidenceSource,
        *,
        signing_key: Any,
        recipient_key: Any,
        capacity: ParticipantCapacityPolicy = DEFAULT_PARTICIPANT_CAPACITY,
        quota_baseline: QuotaBaseline | None = None,
        clock: Callable[[], datetime] | None = None,
        new_id: Callable[[], str] | None = None,
    ) -> None:
        self._transactions, self._source = transactions, source
        self._signing_key, self._recipient_key = signing_key, recipient_key
        self._capacity, self._quota_baseline = capacity, quota_baseline
        self._clock = _clock if clock is None else clock
        self._new_id = _identity if new_id is None else new_id

    async def candidate(self, incoming: FrozenCommandInput) -> FrozenPlan:
        envelope, body = incoming.envelope(), incoming.body_message()
        if envelope.kind != pb.PARTICIPANT_RETRY:
            raise ValueError("Only authenticated original participant retry planning supported")
        operation_id, receipt_id = self._new_id(), self._new_id()
        facts = InterpretationMutationFacts(
            incoming.request.input_hash,
            self._clock(),
            body.participant_retry.session_id,
            run_id=self._new_id(),
            job_id=self._new_id(),
            reservation_token=self._new_id(),
            state_event_id=self._new_id(),
            kind=pb.PARTICIPANT_RETRY,
        )
        recording = RecordingEvidenceSource(self._source, clock=self._clock)
        state = None
        try:
            command = participant_retry_command(body.participant_retry)
            async with self._transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                projection = ReadOnlyParticipantRetries(
                    Transactions.borrowed(db),
                    self._capacity,
                    self._quota_baseline,
                    at=facts.at,
                    run_id=facts.run_id,
                    state_event_id=facts.state_event_id,
                )
                result = await RetryParticipant(projection, recording).execute(command)
                state = projection.state
                facts = replace(facts, audit_digest=projection.audit_digest)
                decision = AdmissionDecision(
                    incoming.organization_id,
                    max(1, result.version),
                    pb.ACCEPTED,
                    workflow_receipt=receipt_message(result),
                )
        except (AccessDenied, NotFound, RuleViolation, ValueError) as error:
            state = None
            facts = replace(facts, audit_digest=None)
            decision = refusal_decision(pb.PARTICIPANT_RETRY, incoming.organization_id, error)
        facts = replace(facts, authorization=recording.proof)
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
            event = prepare(
                pb.INTERPRETATION_STATE,
                state.event_id,
                envelope.aggregate_key,
                pb.MessagingBody(interpretation_state=workflow.StateEvent(**asdict(state))),
                organization_id=incoming.organization_id,
                signing_key=self._signing_key,
                recipient_key=self._recipient_key,
                original_occurred_at=at,
            )
            publications.append((event, state.version))
        plan = freeze_admission_plan(
            incoming, operation_id, tuple(publications), context=facts.encode()
        )
        InterpretationMutationFacts.from_plan(incoming, plan)
        return plan
