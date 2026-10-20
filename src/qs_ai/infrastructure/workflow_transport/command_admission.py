"""Reuse original command rules on the receiver's transaction; never dispatch models."""

from dataclasses import asdict
from datetime import UTC, datetime
from uuid import UUID

from reliable_messaging.durable import MessageConflict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.capacity import CapacityExceeded, EvaluationCapacityPolicy
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.retry import ParticipantRetry, RetryParticipant
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import EditableModelPolicy
from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.application.interpretation.ports import AccessDenied, EvidenceSource, NotFound, Receipt
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.contracts.workflow.command_mapping import receipt_message, scope_from, start_arguments
from qs_ai.domain.interpretation.model import Actor, RuleViolation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.participant_retries import MySQLParticipantRetries
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.infrastructure.workflow_transport.admission_receipts import (
    AdmissionDecision,
    refusal_decision,
    start_decision,
)
from qs_ai.infrastructure.workflow_transport.messaging import FIELDS, valid_number


async def _start_decision(
    tx: Transactions, request: workflow.StartCommand, receipt: Receipt, org: str
) -> AdmissionDecision:
    event: object = None
    if receipt.status == "blocked":
        # Start persists pre-dispatch refusals rather than raising. Read its first
        # event, not mutable Session state: replay must survive later publication
        # or lifecycle changes, including an original request accepted before MQ.
        async with tx.open() as db:
            event = await db.scalar(
                select(result_outbox.c.payload).where(
                    result_outbox.c.session_id == receipt.session_id,
                    result_outbox.c.version == receipt.version,
                )
            )
    return start_decision(request, receipt, org, event)


class WorkflowCommandAdmission:
    def __init__(
        self,
        source: EvidenceSource,
        participant_capacity: ParticipantCapacityPolicy,
        evaluation_capacity: EvaluationCapacityPolicy,
        models: EditableModelPolicy,
        quota_baseline: QuotaBaseline | None,
        runtime_limits: EvaluationRuntimeLimits,
    ) -> None:
        self.source = source
        self.participant_capacity, self.evaluation_capacity = (
            participant_capacity,
            evaluation_capacity,
        )
        self.models, self.quota_baseline = models, quota_baseline
        self.runtime_limits = runtime_limits

    async def admit(
        self, db: AsyncSession, envelope: pb.MessagingEnvelope, body: pb.MessagingBody
    ) -> AdmissionDecision:
        if not db.in_transaction() or body.WhichOneof("value") != FIELDS.get(envelope.kind):
            raise RuntimeError("Authenticated command and original transaction required")
        value = getattr(body, FIELDS[envelope.kind])
        org = (
            value.actor.org_id
            if envelope.kind in (pb.START, pb.CHANGE)
            else str(value.scope.organization_id)
        )
        if not valid_number(org):
            raise MessageConflict("Invalid command organization identity")
        markers = set(db.info.get("evaluation_events", set()))
        try:
            # A deterministic refusal can occur after bounded local writes. Roll
            # those writes back while retaining the receiver's original Inbox lock.
            try:
                async with db.begin_nested():
                    return await self._apply(Transactions.borrowed(db), envelope, body, org)
            except BaseException:
                db.info["evaluation_events"] = markers
                raise
        except (
            AccessDenied,
            NotFound,
            CapacityExceeded,
            CheckpointConflict,
            RuleViolation,
            ValueError,
        ) as error:
            return refusal_decision(envelope.kind, org, error)
        # Database/network/unknown errors deliberately escape to transport REQ.

    async def _apply(
        self,
        tx: Transactions,
        envelope: pb.MessagingEnvelope,
        body: pb.MessagingBody,
        org: str,
    ) -> AdmissionDecision:
        service = InterpretationService(
            MySQLUnitOfWorkFactory(tx, self.participant_capacity, self.quota_baseline, self.models),
            self.source,
        )
        if envelope.kind == pb.START:
            request = body.start
            receipt = await service.start_external(*start_arguments(request))
            return await _start_decision(tx, request, receipt, org)
        elif envelope.kind == pb.CHANGE:
            request = body.change
            if (
                str(UUID(request.command_id)) != request.command_id
                or str(UUID(request.session_id)) != request.session_id
                or request.action not in {"answer", "cancel"}
            ):
                raise ValueError("Invalid command")
            actor = Actor(request.actor.org_id, request.actor.subject_id)
            if request.action == "answer":
                receipt = await service.answer(
                    actor,
                    request.session_id,
                    AnswerCommand(
                        request.expected_version,
                        request.question_id,
                        request.answer if request.HasField("answer") else None,
                        request.skip,
                    ),
                    request.command_id,
                )
            else:
                receipt = await service.cancel(
                    actor,
                    request.session_id,
                    CancelCommand(request.expected_version),
                    request.command_id,
                )
        elif envelope.kind == pb.PARTICIPANT_RETRY:
            request = body.participant_retry
            if request.ByteSize() > 8192:
                raise ValueError("Retry command exceeds limit")
            command = ParticipantRetry(
                DraftScope(request.scope.organization_id, request.scope.operator_user_id),
                request.session_id,
                request.command_id,
                request.expected_run_id,
                request.expected_version,
                request.reason,
                request.confirm,
                request.expected_provider_invocations,
                request.accept_result_unknown_risk,
            )
            receipt = await RetryParticipant(
                MySQLParticipantRetries(tx, self.participant_capacity, self.quota_baseline),
                self.source,
            ).execute(command)
        elif envelope.kind in (pb.EVALUATION_START, pb.EVALUATION_CANCEL):
            request = (
                body.evaluation_start
                if envelope.kind == pb.EVALUATION_START
                else body.evaluation_cancel
            )
            if request.expected_version < 1 or not request.confirm:
                raise ValueError("Explicit version and confirmation required")
            store = MySQLEvaluationManagement(
                tx, self.evaluation_capacity, self.models, self.quota_baseline, self.runtime_limits
            )
            scope = scope_from(request.scope)
            at = datetime.now(UTC)
            if envelope.kind == pb.EVALUATION_START:
                view = await store.start(
                    scope, request.expected_version, request.reason, at, confirm=True
                )
            else:
                if request.ByteSize() > 8192 or not request.HasField("discard"):
                    raise ValueError("Explicit discard decision required")
                view = await store.cancel(
                    scope,
                    request.expected_version,
                    request.reason,
                    at,
                    discard=request.discard,
                    confirm=True,
                )
            return AdmissionDecision(
                org,
                view.version,
                pb.ACCEPTED,
                evaluation_receipt=workflow.EvaluationState(**asdict(view)),
            )
        else:
            raise MessageConflict("Unsupported command kind")
        return AdmissionDecision(
            org,
            max(1, receipt.version),
            pb.ACCEPTED,
            workflow_receipt=receipt_message(receipt),
        )
