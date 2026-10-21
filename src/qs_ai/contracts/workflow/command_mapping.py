"""Shared protobuf mappings; no transport, storage, or business execution ownership."""

from uuid import UUID

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.execution.retry import ParticipantRetry
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.application.interpretation.ports import Receipt
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, Fact


def start_arguments(
    request: pb.StartCommand,
) -> tuple[Actor, str, tuple[str, ...], str, str, tuple[EvidenceItem, ...]]:
    """Copy authenticated START values; no validation result or execution proof."""
    return (
        Actor(request.actor.org_id, request.actor.subject_id),
        request.testee_id,
        tuple(request.assessment_ids),
        request.goal,
        request.request_id,
        tuple(
            EvidenceItem(
                item.assessment_id,
                item.testee_id,
                item.report_id,
                item.source_version,
                tuple(Fact(fact.ref, fact.value) for fact in item.facts),
            )
            for item in request.evidence
        ),
    )


def change_arguments(
    request: pb.ChangeCommand,
) -> tuple[Actor, str, AnswerCommand | CancelCommand, str]:
    """Retain the original CHANGE validation and optional answer semantics."""
    if (
        str(UUID(request.command_id)) != request.command_id
        or str(UUID(request.session_id)) != request.session_id
        or request.action not in {"answer", "cancel"}
    ):
        raise ValueError("Invalid command")
    command: AnswerCommand | CancelCommand = (
        AnswerCommand(
            request.expected_version,
            request.question_id,
            request.answer if request.HasField("answer") else None,
            request.skip,
        )
        if request.action == "answer"
        else CancelCommand(request.expected_version)
    )
    return (
        Actor(request.actor.org_id, request.actor.subject_id),
        request.session_id,
        command,
        request.command_id,
    )


def participant_retry_command(request: pb.ParticipantRetryCommand) -> ParticipantRetry:
    """Copy original explicit retry authority; transport redelivery grants none."""
    if request.ByteSize() > 8192:
        raise ValueError("Retry command exceeds limit")
    return ParticipantRetry(
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


def receipt_message(result: Receipt) -> pb.Receipt:
    return pb.Receipt(
        session_id=result.session_id,
        run_id=result.run_id or "",
        status=result.status,
        version=result.version,
    )


def scope_from(request: pb.EvaluationQuery) -> ManagementScope:
    run_id = UUID(request.run_id)
    if str(run_id) != request.run_id:
        raise ValueError("Canonical Run id required")
    return ManagementScope(run_id, request.organization_id, request.operator_user_id)
