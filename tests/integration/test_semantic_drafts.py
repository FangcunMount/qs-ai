"""Judge editing remains scoped, atomic and independent of mutable deployment files."""

import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.semantic_drafts import (
    CreateSemanticDraft,
    FreezeSemanticDraft,
    ReviseSemanticDraft,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.import_evaluation_assets import baseline_assets
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
    MySQLEvaluationAssets,
    read_semantic_prompt,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    semantic_draft_commands,
    semantic_draft_heads,
    semantic_draft_versions,
    semantic_prompt_assets,
)
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.persistence.mysql.semantic_drafts import MySQLSemanticDrafts
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets

pytestmark = pytest.mark.integration


@pytest.fixture
async def drafts():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    db = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(db)
    scope = DraftScope(uuid4().int % (2**60) + 1, 42)
    source, _, prompt, schema = baseline_assets()
    await MySQLEvaluationAssets(tx).put_semantic_prompt(prompt, source, "test-initialize")
    await MySQLSchemaAssets(tx).put(schema, source, "test-initialize")
    command = CreateSemanticDraft(
        uuid4(),
        uuid4(),
        prompt.reference,
        load_semantic_assets().output_schema,
        0,
        "draft-" + str(uuid4()),
        "裁判草稿测试",
    )
    try:
        yield MySQLSemanticDrafts(tx), scope, command, datetime.now(UTC)
    finally:
        async with tx.open() as session:
            for table in (
                semantic_draft_commands,
                semantic_draft_versions,
                semantic_draft_heads,
                semantic_prompt_assets,
            ):
                await session.execute(
                    delete(table).where(table.c.organization_id == scope.organization_id)
                )
            await session.commit()
        await db.close()


async def test_create_revise_freeze_and_original_receipts(drafts, monkeypatch):
    store, scope, create, at = drafts
    first = await store.apply(scope, create, at)
    edit = ReviseSemanticDraft(
        create.draft_id, uuid4(), 1, first.markdown + "\n版本修订说明", "完善说明"
    )
    second = await store.apply(scope, edit, at + timedelta(seconds=1))
    assert await store.get(scope, create.draft_id, 1) == first
    assert await store.validate(scope, create.draft_id, 2) == second
    freeze = FreezeSemanticDraft(create.draft_id, uuid4(), 2, "冻结待评测版本")
    third = await store.apply(scope, freeze, at + timedelta(seconds=2))
    assert third.state == "frozen" and third.revision == 3
    async with store.transactions.open() as db:
        asset = await read_semantic_prompt(
            db,
            third.asset_reference(),
            owner_organization_id=scope.organization_id,
            requesting_organization_id=scope.organization_id,
        )
        assert asset.markdown == second.markdown

    def unavailable(*args, **kwargs):
        raise AssertionError("Receipt replay must not read current files")

    monkeypatch.setattr("pathlib.Path.read_bytes", unavailable)
    assert await store.apply(scope, create, at) == first
    assert await store.apply(scope, edit, at) == second
    assert await store.apply(scope, freeze, at) == third
    assert await store.receipt(scope, edit.command_id) == second
    with pytest.raises(DraftConflict):
        await store.apply(
            scope, replace(edit, command_id=uuid4(), expected_revision=3), at + timedelta(seconds=3)
        )


async def test_scope_cas_and_command_conflicts(drafts):
    store, scope, create, at = drafts
    first = await store.apply(scope, create, at)
    other = replace(scope, organization_id=scope.organization_id + 1)
    with pytest.raises(NotFound):
        await store.get(other, create.draft_id)
    with pytest.raises(NotFound):
        await store.receipt(replace(scope, operator_user_id=43), create.command_id)
    with pytest.raises(DraftConflict):
        await store.apply(scope, replace(create, reason="different"), at)
    edit = ReviseSemanticDraft(create.draft_id, uuid4(), 1, first.markdown, "并发编辑")
    results = await asyncio.gather(
        store.apply(scope, edit, at),
        store.apply(scope, replace(edit, command_id=uuid4()), at),
        return_exceptions=True,
    )
    assert sum(isinstance(value, DraftConflict) for value in results) == 1


async def test_invalid_placeholder_cannot_freeze_or_create_asset(drafts):
    store, scope, create, at = drafts
    first = await store.apply(scope, create, at)
    broken = first.markdown.replace("semantic_evaluation_payload_json", "arbitrary_secret")
    await store.apply(
        scope, ReviseSemanticDraft(create.draft_id, uuid4(), 1, broken, "未完成编辑"), at
    )
    with pytest.raises(ValueError):
        await store.apply(scope, FreezeSemanticDraft(create.draft_id, uuid4(), 2, "冻结"), at)
    assert (await store.get(scope, create.draft_id)).revision == 2
    async with store.transactions.open() as db:
        assert (
            await db.execute(
                select(func.count())
                .select_from(semantic_prompt_assets)
                .where(semantic_prompt_assets.c.organization_id == scope.organization_id)
            )
        ).scalar_one() == 0


async def test_semantic_draft_rpc_uses_trusted_workload_and_scoped_receipts(drafts, tmp_path):
    import json
    from dataclasses import asdict

    import grpc

    from qs_ai.bootstrap.container import create_container
    from qs_ai.bootstrap.grpc_server import create_grpc_server
    from qs_ai.config import Settings
    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
    from tests.integration.test_delivery import certificates

    _, scope, command, _ = drafts
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
    trusted = pb.PublicationScope(
        organization_id=scope.organization_id, operator_user_id=scope.operator_user_id
    )
    try:
        credentials = grpc.ssl_channel_credentials(
            ca, (tmp_path / "qs.key").read_bytes(), (tmp_path / "qs.pem").read_bytes()
        )
        async with grpc.aio.secure_channel(f"localhost:{port}", credentials) as channel:
            client = rpc.SemanticPromptDraftsStub(channel)
            value = await client.Create(
                pb.SemanticDraftWrite(
                    scope=trusted, command_json=json.dumps(asdict(command), default=str)
                ),
                timeout=5,
            )
            assert value.schema_version == "qs-ai-semantic-draft/v1"
            assert json.loads(value.data_json)["draft"]["revision"] == 1
            receipt = await client.GetReceipt(
                pb.SemanticDraftQuery(scope=trusted, command_id=str(command.command_id)), timeout=5
            )
            assert receipt == value
            await client.Validate(
                pb.SemanticDraftQuery(scope=trusted, draft_id=str(command.draft_id), revision=1),
                timeout=5,
            )
            with pytest.raises(grpc.aio.AioRpcError) as denied:
                await client.Get(
                    pb.SemanticDraftQuery(
                        scope=pb.PublicationScope(
                            organization_id=scope.organization_id + 1,
                            operator_user_id=scope.operator_user_id,
                        ),
                        draft_id=str(command.draft_id),
                    ),
                    timeout=5,
                )
            assert denied.value.code() == grpc.StatusCode.NOT_FOUND
        async with grpc.aio.secure_channel(
            f"localhost:{port}", grpc.ssl_channel_credentials(ca, key, cert)
        ) as channel:
            with pytest.raises(grpc.aio.AioRpcError) as denied:
                await rpc.SemanticPromptDraftsStub(channel).Get(
                    pb.SemanticDraftQuery(scope=trusted, draft_id=str(command.draft_id)), timeout=5
                )
            assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
    finally:
        await server.stop(0)
        await container.close()
