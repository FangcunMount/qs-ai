from typing import Any

import grpc
from dishka import AsyncContainer
from grpc import aio

from qs_ai.application.interpretation.eligibility import EligibilityReader
from qs_ai.application.interpretation.ports import AccessDenied
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.contracts.workflow.command_mapping import receipt_message as receipt_message
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, Fact, RuleViolation
from qs_ai.transport.grpc.mq_cutover import reject_execution


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

    async def Start(
        self, request: pb.StartCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.Receipt:
        return await reject_execution(context)

    async def Change(
        self, request: pb.ChangeCommand, context: aio.ServicerContext[Any, Any]
    ) -> pb.Receipt:
        return await reject_execution(context)
