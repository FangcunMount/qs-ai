"""Real SQL projections preserve business rows and cannot leak foreign sessions."""

import json
import os
from uuid import uuid4

import grpc
import pytest
from sqlalchemy import insert, select

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.grpc_server import create_grpc_server
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.runtime import MySQLRuntimeReader
from qs_ai.infrastructure.persistence.mysql.schema import external_requests, jobs, sessions
from tests.integration.test_delivery import certificates
from tests.integration.test_interpretation import kit as kit

pytestmark = pytest.mark.integration


async def bind(kit):
    receipt = await kit.queued()
    request_id = str(uuid4())
    async with kit.transactions.open() as db:
        await db.execute(
            insert(external_requests).values(request_id=request_id, session_id=receipt.session_id)
        )
        await db.commit()
    return receipt, request_id


async def snapshot(kit, sid):
    async with kit.transactions.open() as db:
        state = (await db.execute(select(sessions).where(sessions.c.id == sid))).mappings().one()
        job = (await db.execute(select(jobs).where(jobs.c.session_id == sid))).mappings().one()
        return dict(state), dict(job)


async def test_runtime_projection_is_read_only_and_body_free(kit):
    receipt, request_id = await bind(kit)
    before = await snapshot(kit, receipt.session_id)
    reader = MySQLRuntimeReader(kit.transactions)
    scope = DraftScope(1, 42)
    batch = await reader.summaries(scope, (receipt.session_id,))
    result = await reader.detail(scope, receipt.session_id)
    assert result["execution"] == batch["items"][0]
    assert result["execution"]["request_id"] == request_id
    assert result["execution"]["publication_id"] is None
    assert result["attempts"][0]["run_id"] == receipt.run_id
    assert result["history_complete"] is False
    assert "synthetic fact" not in json.dumps(result)
    assert "goal" not in result["execution"]
    assert before == await snapshot(kit, receipt.session_id)


async def test_foreign_and_missing_sessions_are_indistinguishable(kit):
    receipt, _ = await bind(kit)
    reader = MySQLRuntimeReader(kit.transactions)
    ids = (receipt.session_id, str(uuid4()))
    result = await reader.summaries(DraftScope(2, 42), ids)
    assert result["items"] == []
    assert result["unavailable_session_ids"] == list(ids)
    for sid in ids:
        with pytest.raises(NotFound):
            await reader.detail(DraftScope(2, 42), sid)


async def test_actual_factory_mtls_and_dependency_graph(kit, tmp_path):
    receipt, request_id = await bind(kit)
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    ca, cert, key = [(tmp_path / n).read_bytes() for n in ("ca.pem", "ai.pem", "ai.key")]
    server = create_grpc_server(
        container,
        Settings(grpc={"bind_address": "localhost:0", "governance_enabled": True}),
        ca,
        cert,
        key,
    )
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()
    try:
        for identity in ("qs", "ai"):
            credentials = grpc.ssl_channel_credentials(
                ca,
                (tmp_path / f"{identity}.key").read_bytes(),
                (tmp_path / f"{identity}.pem").read_bytes(),
            )
            async with grpc.aio.secure_channel(f"localhost:{port}", credentials) as channel:
                client = rpc.RuntimeManagementStub(channel)
                query = pb.RuntimeQuery(
                    scope=pb.PublicationScope(organization_id=1, operator_user_id=42),
                    session_ids=[receipt.session_id],
                )
                if identity == "qs":
                    result = await client.Get(query, timeout=5)
                    assert json.loads(result.data_json)["execution"]["request_id"] == request_id
                else:
                    with pytest.raises(grpc.aio.AioRpcError) as denied:
                        await client.Get(query, timeout=5)
                    assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
    finally:
        await server.stop(0)
        await container.close()


async def test_milestones_share_commits_preserve_unknown_and_expire_only_diagnostics(
    kit, monkeypatch
):
    from sqlalchemy import func, text, update

    from qs_ai.infrastructure.observability import events
    from qs_ai.infrastructure.persistence.mysql.runtime_milestones import prune, record
    from qs_ai.infrastructure.persistence.mysql.schema import model_calls, runtime_milestones

    receipt, _ = await bind(kit)
    claim = await kit.store.claim(30)
    assert claim is not None
    monkeypatch.setattr(
        events.logger, "info", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("log offline"))
    )
    call, dispatched = await kit.store.begin_model_call(claim, '{"fixture":"no provider call"}')
    assert dispatched
    await kit.store.record_model_response(
        claim, call.invocation_id, failure_code="provider_result_unknown", result_unknown=True
    )
    same, dispatched = await kit.store.begin_model_call(claim, "{}")
    assert not dispatched and same.invocation_id == call.invocation_id and same.status == "unknown"
    reader = MySQLRuntimeReader(kit.transactions)
    detail = await reader.detail(DraftScope(1, 42), receipt.session_id)
    assert {v["kind"] for v in detail["milestones"]} == {
        "execution_claimed",
        "model_dispatched",
        "model_unknown",
    }
    async with kit.transactions.open() as db:
        await record(db, receipt.session_id, claim.run_id, "test_rollback", "never-committed")
    detail = await reader.detail(DraftScope(1, 42), receipt.session_id)
    assert all(v["kind"] != "test_rollback" for v in detail["milestones"])
    async with kit.transactions.open() as db:
        await db.execute(
            update(runtime_milestones)
            .where(runtime_milestones.c.session_id == receipt.session_id)
            .values(expires_at=text("UTC_TIMESTAMP(6) - INTERVAL 1 DAY"))
        )
        await db.commit()
    assert await prune(kit.transactions) >= 3
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(model_calls)
                .where(model_calls.c.run_id == claim.run_id)
            )
            == 1
        )
    assert (await reader.detail(DraftScope(1, 42), receipt.session_id))["milestones"] == []


async def test_runtime_health_counts_are_organization_scoped(kit):
    await bind(kit)
    reader = MySQLRuntimeReader(kit.transactions)
    own = await reader.health(DraftScope(1, 42))
    foreign = await reader.health(DraftScope(987654, 42))
    assert own["backlog"]["queued_jobs"] >= 1
    assert all(v == 0 for v in foreign["backlog"].values())
