"""Go audit-authorized planning -> mTLS -> Python -> MySQL; no real IAM or models."""

import asyncio
import json
import os
from dataclasses import asdict

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_planning import execution_counts
from tests.integration.test_evaluation_planning import planning as planning
from tests.integration.test_evaluation_suites import suite_registration as suite_registration
from tests.integration.test_profile_registrations import assets as assets
from tests.integration.test_profile_registrations import complete_release as complete_release
from tests.integration.test_profile_registrations import evaluation_release as evaluation_release
from tests.integration.test_profile_registrations import persisted_assets as persisted_assets
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_profile_registrations import setup_run as setup_run

pytestmark = [pytest.mark.integration, pytest.mark.interop]


async def test_go_plan_is_complete_authorized_read_and_creates_the_same_frozen_run(
    planning, setup_run, go_management, tmp_path
):
    tx, planner, query, _, _ = planning
    expected = asdict(await planner.prepare(query))
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

    async def call(*, certificate="qs", **changes):
        request = dict(
            Action="prepare",
            RunID=str(setup_run[1]),
            OrgID=query.scope.organization_id,
            UserID=query.scope.operator_user_id,
            Allowed=True,
            AuditOnly=True,
            Confirm=True,
            Plan={
                name: asdict(getattr(query, name))
                for name in ("suite", "generation_route", "semantic_route")
            },
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
        before = await execution_counts(tx)
        plan = await call()
        assert plan["Code"] == "OK"
        assert plan["State"] == expected
        assert (await call())["State"] == expected
        assert (await call(Allowed=False))["Denied"]
        assert (await call(OrgID=0))["Invalid"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        bad = {
            name: asdict(getattr(query, name))
            for name in ("suite", "generation_route", "semantic_route")
        }
        bad["semantic_route"]["fingerprint"] = "sha256:" + "0" * 64
        assert (await call(Plan=bad))["Code"] == "InvalidArgument"
        assert await execution_counts(tx) == before
        create = dict(
            Action="create", Release=plan["State"]["release"], Reason="使用准备清单创建冻结评测"
        )
        assert (await call(**create))["Denied"]  # Audit read is not permission to create.
        created = await call(**create, AuditOnly=False)
        assert created["Code"] == "OK"
        assert (
            created["State"]["run_id"],
            created["State"]["status"],
            created["State"]["version"],
        ) == (str(setup_run[1]), "requested", 1)
        assert (await call(**create, AuditOnly=False))["State"] == created["State"]
        assert (await execution_counts(tx))[-1] == before[-1]
    finally:
        await server.stop(0)
        await container.close()
