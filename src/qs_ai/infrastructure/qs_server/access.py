"""Current QS authorization for frozen snapshots, without user access tokens."""

import grpc
from grpc import aio

from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.application.interpretation.service import external_id
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.qs_server.generated.interpretation import interpretation_pb2 as pb
from qs_ai.infrastructure.qs_server.generated.interpretation import interpretation_pb2_grpc as rpc


class QSAccessSource:
    def __init__(self, channel: aio.Channel, timeout: float = 5) -> None:
        if not 0 < timeout <= 30:
            raise ValueError("Authorization deadline must be between zero and thirty seconds")
        self.stub = rpc.AIWorkflowAccessServiceStub(channel)
        self.timeout = timeout

    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None:
        if (
            not external_id(actor.org_id)
            or not actor.subject_id
            or len(actor.subject_id) > 128
            or not external_id(testee_id)
            or not 1 <= len(assessment_ids) <= 10
            or len(set(assessment_ids)) != len(assessment_ids)
            or any(not external_id(value) for value in assessment_ids)
        ):
            raise AccessDenied
        try:
            await self.stub.Authorize(
                pb.AIWorkflowAccessRequest(
                    org_id=actor.org_id,
                    subject_id=actor.subject_id,
                    testee_id=testee_id,
                    assessment_ids=assessment_ids,
                ),
                timeout=self.timeout,
            )
        except aio.AioRpcError as error:
            if error.code() in {
                grpc.StatusCode.PERMISSION_DENIED,
                grpc.StatusCode.UNAUTHENTICATED,
                grpc.StatusCode.NOT_FOUND,
                grpc.StatusCode.INVALID_ARGUMENT,
            }:
                raise AccessDenied from None
            raise DependencyUnavailable("QS current authorization unavailable") from None
