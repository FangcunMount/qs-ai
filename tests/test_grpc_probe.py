from contextlib import asynccontextmanager

import grpc
import pytest

from qs_ai.bootstrap.grpc_probe import probe
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.commands import Commands
from tests.integration.test_delivery import certificates


async def test_real_mtls_probe_accepts_qs_and_rejects_other_identities(tmp_path):
    certificates(tmp_path)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]

    scope_entries = []

    @asynccontextmanager
    async def container():
        scope_entries.append(True)
        yield object()

    server = grpc.aio.server()
    rpc.add_CommandsServicer_to_server(Commands(container), server)
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()
    try:
        address = f"localhost:{port}"
        await probe(
            address,
            ca,
            (tmp_path / "qs.pem").read_bytes(),
            (tmp_path / "qs.key").read_bytes(),
            "FAILED_PRECONDITION",
        )
        await probe(address, ca, cert, key, "PERMISSION_DENIED")
        await probe(address, ca, None, None, "UNAVAILABLE")
        with pytest.raises(
            RuntimeError, match="Expected FAILED_PRECONDITION, received PERMISSION_DENIED"
        ):
            await probe(address, ca, cert, key, "FAILED_PRECONDITION")
        assert not scope_entries, "Transport probe must not enter a business scope"
    finally:
        await server.stop(0)
