"""Real transactional quota configuration: isolation, CAS, history and exact receipts."""

import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, update

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import default_baseline
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.quotas import MySQLQuotas
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_admission_locks,
    organization_quota_commands,
    organization_quota_pointers,
    organization_quota_versions,
    participant_admission_locks,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def quotas():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(database)
    scope = DraftScope(uuid4().int % (2**60) + 1, 42)
    try:
        yield MySQLQuotas(tx, default_baseline()), scope, datetime.now(UTC)
    finally:
        async with tx.open() as db:
            for table in (
                organization_quota_commands,
                organization_quota_pointers,
                organization_quota_versions,
                participant_admission_locks,
                evaluation_admission_locks,
            ):
                await db.execute(
                    delete(table).where(table.c.organization_id == scope.organization_id)
                )
            await db.commit()
        await database.close()


def reduced():
    base = default_baseline().defaults
    return replace(base, participant=replace(base.participant, daily_org=20))


async def test_save_receipt_replay_scope_and_rollback(quotas):
    store, scope, at = quotas
    assert (await store.get(scope))["revision"] == 0
    command = uuid4()
    saved = await store.apply(scope, command, 0, "设置测试额度", at, values=reduced())
    assert saved["effective"]["participant"]["daily_org"] == 20
    assert await store.apply(scope, command, 0, "设置测试额度", at, values=reduced()) == saved
    with pytest.raises(DraftConflict):
        await store.apply(scope, command, 0, "不同正文", at, values=reduced())
    with pytest.raises(NotFound):
        await store.receipt(replace(scope, operator_user_id=43), command)
    assert (await store.get(replace(scope, organization_id=scope.organization_id + 1)))[
        "revision"
    ] == 0
    await store.apply(scope, uuid4(), 1, "恢复部署额度", at, values=default_baseline().defaults)
    rolled = await store.apply(scope, uuid4(), 2, "回到第一版", at, target_revision=1)
    assert rolled["revision"] == 3
    assert rolled["effective"]["participant"]["daily_org"] == 20
    assert [r["revision"] for r in (await store.history(scope))["items"]] == [3, 2, 1]
    assert await store.receipt(scope, command) == saved


async def test_concurrent_first_update_has_one_winner(quotas):
    store, scope, at = quotas
    results = await asyncio.gather(
        *[store.apply(scope, uuid4(), 0, "并发修改", at, values=reduced()) for _ in range(2)],
        return_exceptions=True,
    )
    assert sum(isinstance(r, dict) for r in results) == 1
    assert sum(isinstance(r, DraftConflict) for r in results) == 1


async def test_corruption_fails_and_never_returns_deployment_default(quotas):
    store, scope, at = quotas
    await store.apply(scope, uuid4(), 0, "初始化", at, values=reduced())
    async with store.transactions.open() as db:
        await db.execute(
            update(organization_quota_versions)
            .where(organization_quota_versions.c.organization_id == scope.organization_id)
            .values(definition_sha256="0" * 64)
        )
        await db.commit()
    with pytest.raises(ValueError, match="checksum"):
        await store.get(scope)


async def test_effective_policy_changes_in_same_store_without_restart(quotas):
    from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
    from qs_ai.infrastructure.persistence.mysql.quotas import participant_policy

    store, scope, at = quotas
    async with store.transactions.open() as db:
        before, _ = await participant_policy(db, scope.organization_id, ParticipantCapacityPolicy())
    await store.apply(scope, uuid4(), 0, "即时降额", at, values=reduced())
    async with store.transactions.open() as db:
        after, snapshot = await participant_policy(
            db, scope.organization_id, ParticipantCapacityPolicy()
        )
    assert before.daily_org == 500
    assert after.daily_org == 20
    assert snapshot["revision"] == 1


async def test_current_read_sees_quota_committed_after_old_transaction_snapshot(quotas):
    from sqlalchemy import select

    from qs_ai.infrastructure.persistence.mysql.quotas import effective

    store, scope, at = quotas
    async with store.transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        await db.execute(select(organization_quota_versions))
        await store.apply(scope, uuid4(), 0, "读取当前版本", at, values=reduced())
        result = await effective(db, scope.organization_id, store.baseline)
        assert result["revision"] == 1


async def test_quota_rpc_trusted_workload_and_original_receipt(quotas, tmp_path):
    import json
    from dataclasses import asdict

    import grpc

    from qs_ai.bootstrap.container import create_container
    from qs_ai.bootstrap.grpc_server import create_grpc_server
    from qs_ai.config import Settings
    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
    from tests.integration.test_delivery import certificates

    store, scope, at = quotas
    certificates(tmp_path)
    settings = Settings(
        database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace("mysql://", "mysql+asyncmy://", 1),
        grpc={"governance_enabled": True, "bind_address": "localhost:0"},
    )
    container = create_container(settings)
    ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
    server = create_grpc_server(container, settings, ca, cert, key)
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()
    try:
        credentials = grpc.ssl_channel_credentials(
            ca, (tmp_path / "qs.key").read_bytes(), (tmp_path / "qs.pem").read_bytes()
        )
        async with grpc.aio.secure_channel(
            f"localhost:{port}",
            credentials,
            options=(("grpc.ssl_target_name_override", "localhost"),),
        ) as channel:
            client = rpc.QuotaManagementStub(channel)
            trusted = pb.PublicationScope(
                organization_id=scope.organization_id, operator_user_id=scope.operator_user_id
            )
            command_id = str(uuid4())
            command = {
                "command_id": command_id,
                "expected_revision": 0,
                "reason": "RPC配置测试",
                "values": asdict(reduced()),
            }
            result = await client.Update(
                pb.QuotaWrite(scope=trusted, command_json=json.dumps(command)), timeout=5
            )
            assert json.loads(result.data_json)["revision"] == 1
            receipt = await client.GetReceipt(
                pb.QuotaQuery(scope=trusted, command_id=command_id), timeout=5
            )
            assert receipt == result
            command["expected_revision"] = 1
            command["command_id"] = str(uuid4())
            command["values"]["participant"]["daily_org"] = 999999
            with pytest.raises(grpc.aio.AioRpcError) as invalid:
                await client.Update(
                    pb.QuotaWrite(scope=trusted, command_json=json.dumps(command)), timeout=5
                )
            assert invalid.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        bad = grpc.ssl_channel_credentials(ca, key, cert)
        async with grpc.aio.secure_channel(
            f"localhost:{port}", bad, options=(("grpc.ssl_target_name_override", "localhost"),)
        ) as channel:
            with pytest.raises(grpc.aio.AioRpcError) as denied:
                await rpc.QuotaManagementStub(channel).Get(pb.QuotaQuery(scope=trusted), timeout=5)
            assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
    finally:
        await server.stop(0)
        await container.close()
