"""Generated preflight RPC and actual certificate identity enforcement."""

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.commands import Commands
from tests.integration.test_delivery import certificates
from tests.test_input_binding import bound_case


async def test_mtls_check_is_readonly_and_rejects_untrusted_workload(tmp_path):
    certificates(tmp_path)
    container = create_container(Settings(_env_file=None, database_url=None))
    server = grpc.aio.server()
    rpc.add_CommandsServicer_to_server(Commands(container), server)
    ca, cert, key = [(tmp_path / f).read_bytes() for f in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()
    try:
        for identity in ("qs", "other"):
            credentials = grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            )
            async with grpc.aio.secure_channel(f"localhost:{port}", credentials) as channel:
                client = rpc.CommandsStub(channel)
                request = pb.EligibilityQuery(
                    actor=pb.Actor(org_id="1", subject_id="parent"),
                    testee_id="7",
                    assessment_ids=["42"],
                )
                if identity == "other":
                    with pytest.raises(grpc.aio.AioRpcError) as error:
                        await client.CheckEligibility(request, timeout=3)
                    assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
                    continue
                result = await client.CheckEligibility(request, timeout=3)
                assert (result.status, result.reason_code) == ("unavailable", "source_incomplete")
                _, evidence, _ = bound_case()
                item = evidence.items[0]
                request.evidence.append(
                    pb.EvidenceItem(
                        assessment_id=item.assessment_id,
                        testee_id=item.testee_id,
                        report_id=item.report_id,
                        source_version=item.source_version,
                        facts=[pb.Fact(ref=f.ref, value=f.value) for f in item.facts],
                    )
                )
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await client.CheckEligibility(request, timeout=3)
                assert error.value.code() == grpc.StatusCode.UNAVAILABLE
                assert error.value.details() == "Eligibility temporarily unavailable"
    finally:
        await server.stop(0)
        await container.close()
