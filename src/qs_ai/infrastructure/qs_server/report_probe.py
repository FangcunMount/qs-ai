"""Existing report RPC transport probe, deliberately not an EvidenceSource implementation."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import grpc
from grpc import aio

from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.application.interpretation.service import external_id
from qs_ai.infrastructure.qs_server.generated.interpretation import (
    interpretation_pb2 as pb,
)
from qs_ai.infrastructure.qs_server.generated.interpretation import (
    interpretation_pb2_grpc as rpc,
)


@asynccontextmanager
async def mtls_channel(
    target: str,
    root_ca: bytes,
    private_key: bytes,
    certificate_chain: bytes,
) -> AsyncIterator[aio.Channel]:
    credentials = grpc.ssl_channel_credentials(root_ca, private_key, certificate_chain)
    async with aio.secure_channel(
        target,
        credentials,
        options=(
            ("grpc.enable_retries", 0),
            ("grpc.max_receive_message_length", 2 * 1024 * 1024),
        ),
    ) as channel:
        yield channel


class ParticipantReportProbe:
    def __init__(self, channel: aio.Channel, timeout: float = 5) -> None:
        if timeout <= 0 or timeout > 30:
            raise ValueError("RPC deadline must be between zero and thirty seconds")
        self.stub = rpc.ParticipantReportServiceStub(channel)
        self.timeout = timeout

    async def fetch(
        self, testee_id: str, assessment_id: str, delegation: str
    ) -> pb.AssessmentReport:
        if not external_id(testee_id) or not external_id(assessment_id):
            raise ValueError("Invalid upstream identifier")
        # This token must come from a trusted delegation issuer. Never mint or persist it here.
        if not delegation:
            raise AccessDenied
        try:
            result = await self.stub.GetAssessmentReport(
                pb.GetAssessmentReportRequest(
                    testee_id=int(testee_id), assessment_id=int(assessment_id)
                ),
                metadata=(("x-qs-delegated-subject", delegation),),
                timeout=self.timeout,
            )
        except aio.AioRpcError as error:
            if error.code() in (
                grpc.StatusCode.PERMISSION_DENIED,
                grpc.StatusCode.UNAUTHENTICATED,
                grpc.StatusCode.NOT_FOUND,
            ):
                raise AccessDenied from None
            raise DependencyUnavailable("Report RPC unavailable") from None
        if not result.HasField("report") or result.report.assessment_id != int(assessment_id):
            raise DependencyUnavailable("Report identity missing or inconsistent")
        return result.report
