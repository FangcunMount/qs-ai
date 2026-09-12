"""Actual Go/Python creation with MySQL asset resolution; synthetic IAM snapshot only."""

import asyncio
import copy
import json
import os
from dataclasses import asdict, replace

import grpc
import pytest
from sqlalchemy import delete

from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.import_profiles import baseline_assets as profiles
from qs_ai.bootstrap.import_prompts import baseline_assets as prompts
from qs_ai.bootstrap.import_routes import baseline_assets as routes
from qs_ai.bootstrap.import_schemas import baseline_assets as schemas
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.qs_server.evaluation_suite import V6, V6_PUBLISHED
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_management_interop import go_management as go_management
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.fixture
async def persisted_assets(setup_run):
    tx, *_ = setup_run
    inserted = []
    try:
        for cls, load, table in (
            (MySQLProfileAssets, profiles, profile_assets),
            (MySQLPromptAssets, prompts, prompt_assets),
            (MySQLRouteAssets, routes, route_assets),
            (MySQLSchemaAssets, schemas, schema_assets),
        ):
            source, values = load()
            keys = list(table.primary_key.columns)
            for value in values:
                if await cls(tx).put(value, source, "integration:creation-interop"):
                    condition = (keys[0] == getattr(value, keys[0].name)) & (
                        keys[1] == getattr(value, keys[1].name)
                    )
                    inserted.append((table, condition))
        yield
    finally:
        async with tx.open() as db:
            for table, condition in reversed(inserted):
                await db.execute(delete(table).where(condition))
            await db.commit()


@pytest.mark.parametrize("suite", [V6, V6_PUBLISHED])
async def test_create_replay_conflicts_and_start_use_real_asset_storage(
    setup_run, persisted_assets, complete_release, go_management, tmp_path, suite
):
    complete_release = replace(complete_release, suite=suite)
    tx, run_id, _ = setup_run
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
            RunID=str(run_id),
            Action="create",
            Release=asdict(complete_release),
            Reason="创建跨进程评测",
            OrgID=1,
            UserID=42,
            Allowed=True,
            Confirm=True,
            Version=1,
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
        assert (await call(Allowed=False))["Denied"]
        assert (await call(Confirm=False))["Invalid"]
        assert (await call(certificate="other"))["Code"] == "PermissionDenied"
        bad_release = copy.deepcopy(asdict(complete_release))
        bad_release["prompt"]["fingerprint"] = "sha256:" + "0" * 64
        assert (await call(Release=bad_release))["Code"] == "InvalidArgument"
        assert await rows(tx, run_id) == [None, None, None]
        first, second = await asyncio.gather(call(), call())
        assert first == second and first["Code"] == "OK"
        assert (first["State"]["status"], first["State"]["version"]) == ("requested", 1)
        before = await rows(tx, run_id)
        definition = json.loads(before[0]["definition_json"])
        assert definition["release"] == asdict(complete_release)
        assert definition["audit"]["requested_by"] == "user:42"
        for change in (dict(Reason="不同目的"), dict(OrgID=2), dict(UserID=43)):
            assert (await call(**change))["Code"] == "Aborted"
        assert await rows(tx, run_id) == before
        started = await call(Action="start")
        assert started["Code"] == "OK" and started["State"]["status"] == "collecting"
        after = await rows(tx, run_id)
        replay = await call()
        assert replay["State"] == started["State"]
        assert replay["State"]["version"] == 2
        assert (await call(Action="get"))["State"] == replay["State"]
        assert (await call(Allowed=False))["Denied"]
        assert await rows(tx, run_id) == after
    finally:
        await server.stop(0)
        await container.close()
