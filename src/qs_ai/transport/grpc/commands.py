from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.application.interpretation.eligibility import EligibilityReader
from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable, Receipt
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, Fact, RuleViolation


def receipt_message(result: Receipt) -> pb.Receipt:
    return pb.Receipt(
        session_id=result.session_id,
        run_id=result.run_id or "",
        status=result.status,
        version=result.version,
    )


class Commands(rpc.CommandsServicer):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    async def _authorize(self, context: aio.ServicerContext[Any, Any]) -> None:
        auth = context.auth_context()
        if auth.get("transport_security_type") != [b"ssl"] or auth.get("x509_common_name") != [
            b"qs-apiserver.svc"
        ]:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Untrusted workload")

    async def CheckEligibility(
        self, request: pb.EligibilityQuery, context: aio.ServicerContext[Any, Any]
    ) -> pb.EligibilityStatus:
        await self._authorize(context)
        try:
            actor = Actor(request.actor.org_id, request.actor.subject_id)
            async with self.container() as operation:
                reader = await operation.get(EligibilityReader)
                result = await reader.check(
                    actor,
                    request.testee_id,
                    tuple(request.assessment_ids),
                    tuple(
                        EvidenceItem(
                            item.assessment_id,
                            item.testee_id,
                            item.report_id,
                            item.source_version,
                            tuple(Fact(f.ref, f.value) for f in item.facts),
                        )
                        for item in request.evidence
                    ),
                )
            return pb.EligibilityStatus(status=result.status, reason_code=result.reason_code)
        except AccessDenied:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Resource access denied")
        except (RuleViolation, ValueError):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid eligibility query")
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Eligibility temporarily unavailable")
        raise AssertionError("abort must raise")

    @asynccontextmanager
    async def _translate_errors(
        self, context: aio.ServicerContext[Any, Any]
    ) -> AsyncIterator[None]:
        try:
            yield
            return
        except AccessDenied:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Resource access denied")
        except RuleViolation as error:
            if error.code == "participant_daily_capacity_exceeded":
                await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, error.code)
            code = (
                grpc.StatusCode.ABORTED
                if "conflict" in error.code or error.code == "invalid_state"
                else grpc.StatusCode.INVALID_ARGUMENT
            )
            await context.abort(code, error.code)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid command")
        except DependencyUnavailable:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Business dependencies unavailable")
        except Exception:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, "Command outcome unknown; replay original ID"
            )
        raise AssertionError("abort must raise")

    async def Start(
        self, request: pb.StartCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.Receipt:
        await self._authorize(context)
        async with self._translate_errors(context):
            actor = Actor(request.actor.org_id, request.actor.subject_id)
            async with self.container() as operation:
                service = await operation.get(InterpretationService)
                result = await service.start_external(
                    actor,
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
                            tuple(Fact(f.ref, f.value) for f in item.facts),
                        )
                        for item in request.evidence
                    ),
                )
            return receipt_message(result)
        raise AssertionError("abort must raise")

    async def Change(
        self, request: pb.ChangeCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.Receipt:
        await self._authorize(context)
        async with self._translate_errors(context):
            actor = Actor(request.actor.org_id, request.actor.subject_id)
            async with self.container() as operation:
                service = await operation.get(InterpretationService)
                if (
                    str(UUID(request.command_id)) != request.command_id
                    or str(UUID(request.session_id)) != request.session_id
                    or request.action not in {"answer", "cancel"}
                ):
                    raise ValueError("Invalid command")
                if request.action == "answer":
                    answer = AnswerCommand(
                        expected_version=request.expected_version,
                        question_id=request.question_id,
                        answer=request.answer if request.HasField("answer") else None,
                        skip=request.skip,
                    )
                    result = await service.answer(
                        actor, request.session_id, answer, request.command_id
                    )
                else:
                    result = await service.cancel(
                        actor,
                        request.session_id,
                        CancelCommand(request.expected_version),
                        request.command_id,
                    )
            return receipt_message(result)
        raise AssertionError("abort must raise")
