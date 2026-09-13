"""QS application/client -> real mTLS -> Python/MySQL catalog; synthetic IAM only."""

import asyncio
import json
import os
from dataclasses import asdict

import grpc
import pytest

from qs_ai.application.evaluation.catalog import EvaluationCatalogQuery
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_catalog import catalog_runs as catalog_runs
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_catalog_scope_cursor_summary_and_detail(catalog_runs, go_management, tmp_path):
    tx, ids, catalog = catalog_runs
    before = [await rows(tx, run_id) for run_id in ids]
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_EvaluationManagementServicer_to_server(EvaluationManagement(container), server)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()

    async def call(certificate="qs", **changes):
        request = dict(
            Action="list",
            OrgID=1,
            UserID=42,
            Allowed=True,
            AuditOnly=True,
            Catalog={"status": "requested", "limit": 2},
        )
        request.update(changes)
        process = await asyncio.create_subprocess_exec(
            str(go_management),
            f"localhost:{port}",
            str(tmp_path / "ca.pem"),
            str(tmp_path / f"{certificate}.pem"),
            str(tmp_path / f"{certificate}.key"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(json.dumps(request).encode()), 15
            )
            assert process.returncode == 0
            return json.loads(output)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    try:
        first = await call()
        assert first["Code"] == "OK"
        expected = json.loads(
            json.dumps(asdict(await catalog.list(EvaluationCatalogQuery(1, 42, "requested", 2))))
        )
        assert first["State"] == expected
        assert [item["run_id"] for item in first["State"]["items"]] == [str(ids[2]), str(ids[1])]
        next_query = {"status": "requested", "limit": 2, "cursor": first["State"]["next_cursor"]}
        second = await call(Catalog=next_query)
        assert second["Code"] == "OK"
        assert [item["run_id"] for item in second["State"]["items"]] == [str(ids[0])]
        assert second["State"]["next_cursor"] == ""
        assert (await call(OrgID=2, Catalog=next_query))["Invalid"]
        assert (await call(Catalog={**next_query, "status": "approved"}))["Invalid"]
        assert (await call(Allowed=False))["Denied"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        assert (await call(UserID=-1))["Invalid"]
        other = await call(OrgID=2)
        assert [item["run_id"] for item in other["State"]["items"]] == [str(ids[3])]
        selected = first["State"]["items"][0]
        detail = await call(Action="get", RunID=selected["run_id"])
        assert detail["Code"] == "OK"
        assert detail["State"]["run_id"] == selected["run_id"]
        assert detail["State"]["version"] == selected["version"]
        assert (
            await call(
                Action="start", RunID=selected["run_id"], Version=selected["version"], Confirm=True
            )
        )["Denied"]
        assert [await rows(tx, run_id) for run_id in ids] == before
    finally:
        await server.stop(0)
        await container.close()
