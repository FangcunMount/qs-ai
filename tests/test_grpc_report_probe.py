import asyncio
from datetime import UTC, datetime, timedelta

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from grpc import aio

from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.infrastructure.qs_server.generated.interpretation import interpretation_pb2 as pb
from qs_ai.infrastructure.qs_server.generated.interpretation import interpretation_pb2_grpc as rpc
from qs_ai.infrastructure.qs_server.report_probe import ParticipantReportProbe, mtls_channel


@pytest.fixture(scope="module")
def certificate():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return (
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
        cert.public_bytes(serialization.Encoding.PEM),
    )


@pytest.mark.parametrize("mode", ["success", "denied", "missing", "wrong_id", "timeout"])
async def test_real_mtls_rpc_and_failure_mapping(certificate, mode):
    received = []

    class Server(rpc.ParticipantReportServiceServicer):
        async def GetAssessmentReport(self, request, context):
            assert context.auth_context().get("x509_common_name") == [b"localhost"]
            assert (
                dict(context.invocation_metadata())["x-qs-delegated-subject"] == "test-delegation"
            )
            received.append(request)
            if mode == "denied":
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "private upstream detail")
            if mode == "timeout":
                await asyncio.sleep(0.2)
            if mode == "missing":
                return pb.GetAssessmentReportResponse()
            return pb.GetAssessmentReportResponse(
                report=pb.AssessmentReport(
                    assessment_id=1 if mode == "wrong_id" else request.assessment_id,
                    conclusion="synthetic projection",
                )
            )

    key, cert = certificate
    server = aio.server()
    rpc.add_ParticipantReportServiceServicer_to_server(Server(), server)
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials(
            ((key, cert),), root_certificates=cert, require_client_auth=True
        ),
    )
    await server.start()
    try:
        async with mtls_channel(f"localhost:{port}", cert, key, cert) as channel:
            await asyncio.wait_for(channel.channel_ready(), 5)
            client = ParticipantReportProbe(channel, timeout=0.05 if mode == "timeout" else 2)
            if mode == "success":
                result = await client.fetch("18446744073709551615", "42", "test-delegation")
                assert result.assessment_id == 42
                assert received[0].testee_id == 2**64 - 1
                # Existing display protocol must not silently become immutable evidence.
                assert "report_id" not in result.DESCRIPTOR.fields_by_name
            else:
                error_type = AccessDenied if mode == "denied" else DependencyUnavailable
                with pytest.raises(error_type) as error:
                    await client.fetch("7", "42", "test-delegation")
                assert "private upstream detail" not in str(error.value)
    finally:
        await server.stop(0)
