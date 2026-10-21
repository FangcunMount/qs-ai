"""Freeze actual evaluation start/cancel results before signing and preparing halves."""

from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

from reliable_messaging.transaction import FrozenPlan
from sqlalchemy import select

from qs_ai.application.evaluation.capacity import CapacityExceeded, EvaluationCapacityPolicy
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.execution_mode import (
    SERIAL_RUNTIME_LIMITS,
    EvaluationRuntimeLimits,
)
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import (
    DEFAULT_EDITABLE_MODELS,
    EditableModelPolicy,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.contracts.workflow.command_mapping import evaluation_arguments
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_admission import (
    EvaluationAdmissionProjection,
)
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    build_receipt_body,
    refusal_decision,
)
from qs_ai.infrastructure.workflow_transport.evaluation_plan import EvaluationPlanFacts
from qs_ai.infrastructure.workflow_transport.messaging import FIELDS, prepare
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    freeze_admission_plan,
)

_DEFAULT_CAPACITY = EvaluationCapacityPolicy()


def _clock() -> datetime:
    return datetime.now(UTC)


def _identity() -> str:
    return str(uuid4())


class EvaluationAdmissionPlanner:
    def __init__(
        self,
        transactions: Transactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        capacity: EvaluationCapacityPolicy = _DEFAULT_CAPACITY,
        models: EditableModelPolicy = DEFAULT_EDITABLE_MODELS,
        quota_baseline: QuotaBaseline | None = None,
        runtime_limits: EvaluationRuntimeLimits = SERIAL_RUNTIME_LIMITS,
        clock: Callable[[], datetime] | None = None,
        new_id: Callable[[], str] | None = None,
    ) -> None:
        self._transactions = transactions
        self._signing_key, self._recipient_key = signing_key, recipient_key
        self._capacity, self._models = capacity, models
        self._quota_baseline, self._runtime_limits = quota_baseline, runtime_limits
        self._clock = _clock if clock is None else clock
        self._new_id = _identity if new_id is None else new_id

    async def candidate(self, incoming: FrozenCommandInput) -> FrozenPlan:
        envelope, body = incoming.envelope(), incoming.body_message()
        if envelope.kind not in (pb.EVALUATION_START, pb.EVALUATION_CANCEL):
            raise ValueError("Only original evaluation start/cancel planning supported")
        request = getattr(body, FIELDS[envelope.kind])
        operation_id, receipt_id = self._new_id(), self._new_id()
        facts = EvaluationPlanFacts(
            incoming.request.input_hash,
            envelope.kind,
            self._clock(),
            request.scope.run_id,
            self._new_id(),
        )
        state = None
        try:
            scope, version, reason, discard = evaluation_arguments(request)
            async with self._transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                projection = EvaluationAdmissionProjection(
                    Transactions.borrowed(db),
                    self._capacity,
                    self._models,
                    self._quota_baseline,
                    self._runtime_limits,
                )
                try:
                    if discard is None:
                        view = await projection.start(
                            scope, version, reason, facts.at, confirm=True
                        )
                    else:
                        view = await projection.cancel(
                            scope, version, reason, facts.at, discard=discard, confirm=True
                        )
                finally:
                    facts = replace(facts, source_digest=projection.source_digest)
                state = projection.state
                if state is None:
                    raise RuntimeError("Original evaluation projection missing")
                previous = (
                    (
                        await db.execute(
                            select(evaluation_sequences).where(
                                evaluation_sequences.c.run_id == str(scope.run_id)
                            )
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if previous is not None:
                    facts = replace(
                        facts,
                        previous_sequence=previous["sequence"],
                        previous_version=previous["version"],
                    )
                if facts.previous_version > version:
                    raise RuntimeError("Original evaluation event version regressed")
                decision = AdmissionDecision(
                    incoming.organization_id,
                    view.version,
                    pb.ACCEPTED,
                    evaluation_receipt=workflow.EvaluationState(**asdict(view)),
                )
        except (NotFound, CapacityExceeded, CheckpointConflict, ValueError) as error:
            state = None
            decision = refusal_decision(envelope.kind, incoming.organization_id, error)
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
                pb.EVALUATION_STATE,
                str(uuid5(UUID(state.run_id), f"state:{state.version}")),
                envelope.aggregate_key,
                pb.MessagingBody(
                    evaluation_state=pb.EvaluationRuntimeState(
                        **asdict(state), event_sequence=facts.previous_sequence + 1
                    )
                ),
                organization_id=incoming.organization_id,
                signing_key=self._signing_key,
                recipient_key=self._recipient_key,
                original_occurred_at=at,
            )
            publications.append((event, facts.previous_sequence + 1))
        plan = freeze_admission_plan(
            incoming, operation_id, tuple(publications), context=facts.encode()
        )
        EvaluationPlanFacts.from_plan(incoming, plan)
        return plan
