"""Real Go application/client -> Python gRPC -> MySQL; IAM snapshot is synthetic."""

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import grpc
import pytest
from sqlalchemy import delete

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import reserve_dispatch
from qs_ai.infrastructure.persistence.mysql.evaluation_preparation import prepare_execution
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import (
    execute_preflight,
    transition_requested,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_recovery import recover_expired
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_evaluation_case import release as case_release
from tests.test_generation_completion_assets import assets

pytestmark = [pytest.mark.integration, pytest.mark.interop]


@pytest.fixture
async def go_management(tmp_path):
    source = os.getenv("QS_AI_GOVERNANCE_SOURCE")
    if not source or not shutil.which("go"):
        pytest.skip("Requires isolated QS governance checkout and Go")
    root = Path(source)
    with tempfile.TemporaryDirectory(prefix="qs_ai_management_", dir=root / "scripts") as directory:
        program = Path(directory) / "main.go"
        shutil.copyfile(Path(__file__).parents[1] / "fixtures/go_evaluation_management.go", program)
        binary = tmp_path / "management"
        process = await asyncio.create_subprocess_exec(
            "go",
            "build",
            "-o",
            str(binary),
            str(program),
            cwd=root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, error = await asyncio.wait_for(process.communicate(), 120)
            assert process.returncode == 0, error.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
    return binary


@pytest.fixture
async def realtime_run(setup_run, decision):
    tx, run_id, release = setup_run
    refs, _, routes, schemas = assets()
    semantic = load_semantic_assets()
    release = replace(
        release,
        profile=case_release().profile,
        prompt=case_release().prompt,
        generation_route=refs.generation_route,
        output_schema=refs.output_schema,
        semantic_route=refs.generation_route,
        semantic_prompt=semantic.prompt,
        semantic_output_schema=semantic.output_schema,
    )
    at = datetime.now(UTC) - timedelta(minutes=10)
    if decision == "start":
        async with tx.open() as db:
            state = await create_run(db, run_id, release, 1, "user:42", "跨进程管理测试", at)
            await db.commit()
        yield tx, run_id, state
        return
    async with tx.open() as db:
        await create_run(db, run_id, release, 1, "user:42", "跨进程管理测试", at)
        await transition_requested(db, run_id, 1, 1, "collecting", "user:42", "开始", at)
        await execute_preflight(db, run_id, 2, 1, at)
        state = await prepare_execution(
            db,
            run_id,
            3,
            1,
            "worker:expired",
            "execution:dead",
            "invocation:dead",
            at,
            at + timedelta(minutes=5),
        )
        state = await reserve_dispatch(db, run_id, state.version, "worker:expired", at)
        state = await recover_expired(
            db,
            run_id,
            state.version,
            1,
            "invocation:dead",
            at + timedelta(minutes=5),
            at + timedelta(minutes=6),
            "system:recovery",
            routes,
            schemas,
        )
        await db.commit()
    try:
        yield tx, run_id, state
    finally:
        async with tx.open() as db:
            for table in (evaluation_semantic_completions, evaluation_generation_completions):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()


@pytest.mark.parametrize("decision", ["cancel_run", "authorize_replacement", "start"])
async def test_go_python_governance_mtls_resolution_and_readback(
    realtime_run, go_management, tmp_path, decision
):
    tx, run_id, state = realtime_run
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
            Action="get",
            Decision=decision,
            OrgID=1,
            Version=state.version,
            Allowed=True,
            Confirm=True,
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

    action = "start" if decision == "start" else "resolve"
    try:
        initial = await call()
        assert initial["Code"] == "OK"
        assert initial["State"]["status"] == ("requested" if decision == "start" else "blocked")
        assert (await call(Allowed=False, Action=action))["Denied"]
        assert (await call(Confirm=False, Action=action))["Invalid"]
        assert (await call(OrgID=2))["Code"] == "NotFound"
        assert (await call(certificate="other", Action=action))["Code"] == "PermissionDenied"
        assert (await rows(tx, run_id))[2]["version"] == state.version
        accepted = await call(Action=action)
        assert accepted["Code"] == "OK"
        assert accepted["State"]["status"] == (
            "canceled" if decision == "cancel_run" else "collecting"
        )
        readback = await call()
        assert readback["State"] == accepted["State"]
        if decision == "start":
            persisted = (await rows(tx, run_id))[0]["progress_json"]
            assert persisted["transitions"][-1]["actor"] == "user:42"
            assert persisted["transitions"][-1]["cause_code"] == "evaluation_started"
            assert readback["State"]["version"] == state.version + 1
            assert (await call(Action=action, Version=state.version + 1))["Code"] == "Aborted"
        else:
            assert readback["State"]["resolutions"][0]["actor"] == "user:42"
        assert (await call(Action=action))["Code"] == "Aborted"
        assert (await call(Allowed=False))["Denied"]
    finally:
        await server.stop(0)
        await container.close()
