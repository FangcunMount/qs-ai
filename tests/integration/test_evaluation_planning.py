"""A read-only plan resolves real stored assets and can create a frozen Run unchanged."""

import json
import os
from dataclasses import asdict, replace

import grpc
import pytest
from sqlalchemy import func, select

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.evaluation.planning import EvaluationPlanQuery
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.evaluation_planning import MySQLEvaluationPlanner
from qs_ai.infrastructure.persistence.mysql.evaluation_requests import MySQLEvaluationRequests
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_run_policies,
    evaluation_runs,
)
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED
from qs_ai.transport.grpc.evaluation import EvaluationManagement
from tests.integration.test_delivery import certificates
from tests.integration.test_evaluation_suites import suite_registration as suite_registration
from tests.integration.test_profile_registrations import assets as assets
from tests.integration.test_profile_registrations import complete_release as complete_release
from tests.integration.test_profile_registrations import evaluation_release as evaluation_release
from tests.integration.test_profile_registrations import persisted_assets as persisted_assets
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_profile_registrations import setup_run as setup_run

pytestmark = pytest.mark.integration


@pytest.fixture
async def planning(suite_registration):
    tx, registrar, scope, command, at, _, release = suite_registration
    receipt = await registrar.register(scope, command, at)
    query = EvaluationPlanQuery(
        scope, receipt.suite, release.generation_route, release.semantic_route
    )
    return tx, MySQLEvaluationPlanner(tx), query, at, replace(release, suite=receipt.suite)


async def execution_counts(tx):
    async with tx.open() as db:
        return tuple(
            [
                (await db.execute(select(func.count()).select_from(table))).scalar_one()
                for table in (
                    evaluation_runs,
                    evaluation_checkpoints,
                    evaluation_run_policies,
                    evaluation_dispatches,
                )
            ]
        )


async def test_native_plan_resolves_all_references_without_creating_or_dispatching(
    planning, setup_run
):
    tx, planner, query, at, release = planning
    before = await execution_counts(tx)
    plan = await planner.prepare(query)
    assert plan.release == release
    assert plan.release_fingerprint == release.fingerprint()
    assert (
        plan.generation_case_count,
        plan.candidates_per_case,
        plan.candidate_count,
        plan.preflight_case_count,
    ) == (7, 5, 35, 1)
    assert (plan.max_generation_invocations, plan.max_semantic_invocations) == (70, 70)
    release.validate_frozen_policies(plan.execution_policy_json, plan.gate_policy_json)
    assert plan == await planner.prepare(query)
    assert await execution_counts(tx) == before
    # The returned eleven references are directly usable, with no browser reconstruction.
    creator = MySQLRunCreator(
        tx,
        MySQLProfileAssets(tx),
        MySQLPromptAssets(tx),
        MySQLRouteAssets(tx),
        MySQLSchemaAssets(tx),
    )
    view = await MySQLEvaluationRequests(tx, creator).create(
        ManagementScope(setup_run[1], query.scope.organization_id, query.scope.operator_user_id),
        plan.release,
        "使用确认的只读计划创建评测",
        at,
        confirm=True,
    )
    assert (view.status, view.version) == ("requested", 1)
    assert (await execution_counts(tx))[-1] == before[-1]


async def test_migrated_baseline_plan_resolves_existing_assets(planning, complete_release):
    _, planner, query, _, _ = planning
    plan = await planner.prepare(replace(query, suite=V6_PUBLISHED))
    assert plan.release == replace(complete_release, suite=V6_PUBLISHED)
    assert plan.candidate_count == 35


@pytest.mark.parametrize("component", ["suite", "generation_route", "semantic_route"])
async def test_mismatched_selected_reference_is_rejected_without_writes(planning, component):
    tx, planner, query, _, _ = planning
    bad = replace(getattr(query, component), fingerprint="sha256:" + "0" * 64)
    before = await execution_counts(tx)
    with pytest.raises(ValueError):
        await planner.prepare(replace(query, **{component: bad}))
    assert await execution_counts(tx) == before


@pytest.fixture
async def planning_rpc(planning, tmp_path):
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

    def channel(identity="qs"):
        return grpc.aio.secure_channel(
            f"localhost:{port}",
            grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            ),
        )

    try:
        yield channel
    finally:
        await server.stop(0)
        await container.close()


async def test_mtls_rpc_and_dishka_return_plan_and_reject_invalid_scope(planning, planning_rpc):
    tx, planner, query, _, _ = planning
    request = pb.EvaluationPlanQuery(
        scope=pb.PublicationScope(**asdict(query.scope)),
        **{
            field: pb.FrozenEvaluationRef(**asdict(getattr(query, field)))
            for field in ("suite", "generation_route", "semantic_route")
        },
    )
    before = await execution_counts(tx)
    async with planning_rpc() as channel:
        client = rpc.EvaluationManagementStub(channel)
        result = await client.Prepare(request, timeout=5)
        assert result.schema_version == "qs-ai-evaluation-plan/v1"
        assert json.loads(result.plan_json) == asdict(await planner.prepare(query))
        invalid = pb.EvaluationPlanQuery()
        invalid.CopyFrom(request)
        invalid.scope.organization_id = 0
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Prepare(invalid, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        invalid.CopyFrom(request)
        invalid.suite.id = "x" * 9000
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await client.Prepare(invalid, timeout=5)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    async with planning_rpc("ai") as channel:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await rpc.EvaluationManagementStub(channel).Prepare(request, timeout=5)
        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert await execution_counts(tx) == before
