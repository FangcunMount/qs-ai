"""Real QS authorization adapter -> mTLS -> Python -> MySQL, with synthetic test IAM."""

import asyncio
import json
import os
from dataclasses import asdict

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.grpc_server import create_grpc_server
from qs_ai.config import Settings
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_semantic_drafts import drafts as drafts

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_semantic_draft_write_read_and_authorization(drafts, go_management, tmp_path):
    _, scope, command, _ = drafts
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            ),
            grpc={"governance_enabled": True, "bind_address": "localhost:0"},
        )
    )
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    settings = Settings(grpc={"governance_enabled": True, "bind_address": "localhost:0"})
    server = create_grpc_server(container, settings, ca, cert, key)
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials(
            [(key, cert)],
            root_certificates=ca,
            require_client_auth=True,
        ),
    )
    await server.start()

    async def invoke(**changes):
        payload = dict(
            Action="semantic-draft",
            Allowed=True,
            OrgID=scope.organization_id,
            UserID=scope.operator_user_id,
            SemanticWrite=True,
            SemanticOperation="create",
            SemanticID=str(command.draft_id),
            SemanticBody=asdict(command),
        )
        payload.update(changes)
        process = await asyncio.create_subprocess_exec(
            str(go_management),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / "qs.pem"),
            str(tmp_path / "qs.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, error = await asyncio.wait_for(
                process.communicate(json.dumps(payload, default=str).encode()), 30
            )
            assert process.returncode == 0, error.decode()
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    try:
        assert (await invoke(Allowed=False))["Denied"]
        assert (await invoke(AuditOnly=True))["Denied"]
        created = await invoke()
        assert created["Code"] == "OK"
        assert created["State"]["draft"]["revision"] == 1
        assert await invoke() == created
        read = await invoke(SemanticWrite=False, SemanticOperation="get", AuditOnly=True)
        assert read == created
        receipt = await invoke(
            SemanticWrite=False, SemanticOperation="receipt", SemanticID=str(command.command_id)
        )
        assert receipt == created
        assert (
            await invoke(
                SemanticWrite=False, SemanticOperation="get", OrgID=scope.organization_id + 1
            )
        )["Code"] == "NotFound"
    finally:
        await server.stop(0)
        await container.close()
