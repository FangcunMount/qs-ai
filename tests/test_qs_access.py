from unittest.mock import AsyncMock

import grpc
import pytest

from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.qs_server.access import QSAccessSource
from qs_ai.infrastructure.qs_server.generated.interpretation import interpretation_pb2 as pb
from qs_ai.infrastructure.qs_server.generated.interpretation import interpretation_pb2_grpc as rpc
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from tests.integration.test_delivery import certificates


async def test_current_authorization_over_real_mtls_rechecks_and_fails_closed(tmp_path):
    certificates(tmp_path)
    ca, qs_cert, qs_key, ai_cert, ai_key = [
        (tmp_path / name).read_bytes()
        for name in ("ca.pem", "qs.pem", "qs.key", "ai.pem", "ai.key")
    ]

    class Access(rpc.AIWorkflowAccessServiceServicer):
        code = grpc.StatusCode.OK
        calls = 0

        async def Authorize(self, request, context):
            self.calls += 1
            assert context.auth_context()["x509_common_name"] == [b"qs-ai.svc"]
            assert request == pb.AIWorkflowAccessRequest(
                org_id="1", subject_id="parent", testee_id="7", assessment_ids=["42"]
            )
            assert not any(
                key == "x-qs-delegated-subject" for key, _ in context.invocation_metadata()
            )
            if self.code != grpc.StatusCode.OK:
                await context.abort(self.code, "upstream sensitive diagnostic")
            return pb.AIWorkflowAccessResponse()

    handler = Access()
    server = grpc.aio.server()
    rpc.add_AIWorkflowAccessServiceServicer_to_server(handler, server)
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials(
            [(qs_key, qs_cert)], root_certificates=ca, require_client_auth=True
        ),
    )
    await server.start()
    try:
        async with mtls_channel(f"localhost:{port}", ca, ai_key, ai_cert) as channel:
            source = QSAccessSource(channel)
            await source.authorize(Actor("1", "parent"), "7", ("42",))
            for code, error in [
                (grpc.StatusCode.PERMISSION_DENIED, AccessDenied),
                (grpc.StatusCode.UNAUTHENTICATED, AccessDenied),
                (grpc.StatusCode.NOT_FOUND, AccessDenied),
                (grpc.StatusCode.INVALID_ARGUMENT, AccessDenied),
                (grpc.StatusCode.UNAVAILABLE, DependencyUnavailable),
                (grpc.StatusCode.DEADLINE_EXCEEDED, DependencyUnavailable),
                (grpc.StatusCode.INTERNAL, DependencyUnavailable),
            ]:
                handler.code = code
                with pytest.raises(error) as raised:
                    await source.authorize(Actor("1", "parent"), "7", ("42",))
                assert "sensitive" not in str(raised.value)
            assert handler.calls == 8
    finally:
        await server.stop(0)


async def test_invalid_identity_does_not_call_qs():
    async with grpc.aio.insecure_channel("localhost:1") as channel:
        source = QSAccessSource(channel)
        source.stub.Authorize = AsyncMock()
        for ids in [(), ("42", "42"), ("0",), ("42",) * 11]:
            with pytest.raises(AccessDenied):
                await source.authorize(Actor("1", "parent"), "7", ids)
        source.stub.Authorize.assert_not_called()
