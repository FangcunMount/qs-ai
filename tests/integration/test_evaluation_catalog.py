"""Disposable MySQL summaries; synthetic Runs, no model execution or production authority."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select, update

from qs_ai.application.evaluation.catalog import EvaluationCatalogQuery
from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_catalog import MySQLEvaluationCatalog
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_generation_completions,
    evaluation_run_policies,
    evaluation_runs,
)
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run

pytestmark = pytest.mark.integration
AT = datetime(2026, 9, 13, 8, tzinfo=UTC)


@pytest.fixture
async def catalog_runs(setup_run):
    tx, _, release = setup_run
    ids = sorted([uuid4() for _ in range(4)], key=str)
    async with tx.open() as db:
        for i, run_id in enumerate(ids):
            at = AT if i in (1, 2) else AT - timedelta(seconds=1)
            if i == 2:
                at = at.astimezone(timezone(timedelta(hours=8)))
            await create_run(db, run_id, release, 2 if i == 3 else 1, "user:42", "目录验证", at)
        await db.commit()
    try:
        yield tx, ids, MySQLEvaluationCatalog(tx)
    finally:
        async with tx.open() as db:
            for table in (
                evaluation_generation_completions,
                evaluation_checkpoints,
                evaluation_run_policies,
                evaluation_runs,
            ):
                await db.execute(delete(table).where(table.c.run_id.in_([str(v) for v in ids])))
            await db.commit()


async def test_keyset_scope_tied_times_timezone_and_read_only(catalog_runs):
    tx, ids, catalog = catalog_runs
    before = [await rows(tx, value) for value in ids]
    query = EvaluationCatalogQuery(1, 42, limit=2)
    first = await catalog.list(query)
    assert [item.run_id for item in first.items] == [str(ids[2]), str(ids[1])]
    assert all(item.created_at == AT.isoformat() for item in first.items)
    assert all(item.status == "requested" and item.version == 1 for item in first.items)
    assert first.items[0].profile_id == "profile"
    assert first.next_cursor
    second = await catalog.list(replace(query, cursor=first.next_cursor))
    assert [item.run_id for item in second.items] == [str(ids[0])]
    assert second.next_cursor == ""
    assert [await rows(tx, value) for value in ids] == before
    other = await catalog.list(EvaluationCatalogQuery(2, 43))
    assert [item.run_id for item in other.items] == [str(ids[3])]
    assert (await catalog.list(EvaluationCatalogQuery(3, 42))).items == ()


async def test_status_filter_and_current_version(catalog_runs):
    tx, ids, catalog = catalog_runs
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(ids[0]))
            .values(progress_json={"status": "blocked", "unresolved_result_unknown_count": 2})
        )
        await db.execute(
            update(evaluation_checkpoints)
            .where(evaluation_checkpoints.c.run_id == str(ids[0]))
            .values(version=7)
        )
        await db.commit()
    page = await catalog.list(EvaluationCatalogQuery(1, 42, "blocked"))
    assert len(page.items) == 1
    assert (
        page.items[0].run_id,
        page.items[0].version,
        page.items[0].unresolved_result_unknown_count,
    ) == (str(ids[0]), 7, 2)
    assert len((await catalog.list(EvaluationCatalogQuery(1, 42, "requested"))).items) == 2
    assert (await catalog.list(EvaluationCatalogQuery(1, 42, "approved"))).items == ()


async def test_newer_insert_does_not_repeat_existing_page(catalog_runs):
    tx, ids, catalog = catalog_runs
    query = EvaluationCatalogQuery(1, 42, limit=2)
    first = await catalog.list(query)
    # This row was not in the first organization's page; move its creation later.
    async with tx.open() as db:
        raw = (
            await db.execute(
                select(evaluation_runs.c.definition_json).where(
                    evaluation_runs.c.run_id == str(ids[3])
                )
            )
        ).scalar_one()
        definition = json.loads(raw)
        definition["audit"]["organization_id"] = 1
        definition["audit"]["created_at"] = (AT + timedelta(seconds=1)).isoformat()
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(ids[3]))
            .values(organization_id=1, definition_json=json.dumps(definition))
        )
        await db.commit()
    second = await catalog.list(replace(query, cursor=first.next_cursor))
    assert [item.run_id for item in second.items] == [str(ids[0])]
    assert [item.run_id for item in (await catalog.list(query)).items] == [str(ids[3]), str(ids[2])]


@pytest.mark.parametrize("damage", ["checkpoint", "audit", "time", "status"])
async def test_damaged_summary_is_not_silently_accepted(catalog_runs, damage):
    tx, ids, catalog = catalog_runs
    async with tx.open() as db:
        target = evaluation_runs.c.run_id == str(ids[2])
        if damage == "checkpoint":
            await db.execute(
                delete(evaluation_checkpoints).where(evaluation_checkpoints.c.run_id == str(ids[2]))
            )
        elif damage == "status":
            await db.execute(
                update(evaluation_runs).where(target).values(progress_json={"status": "invented"})
            )
        else:
            raw = (
                await db.execute(select(evaluation_runs.c.definition_json).where(target))
            ).scalar_one()
            definition = json.loads(raw)
            if damage == "audit":
                definition["audit"]["organization_id"] = 99
            else:
                definition["audit"]["created_at"] = "broken"
            await db.execute(
                update(evaluation_runs).where(target).values(definition_json=json.dumps(definition))
            )
        await db.commit()
    with pytest.raises((ValueError, CheckpointConflict)):
        await catalog.list(EvaluationCatalogQuery(1, 42))


async def test_catalog_uses_real_mtls_and_container_scope(catalog_runs, tmp_path):
    import os

    import grpc

    from qs_ai.bootstrap.container import create_container
    from qs_ai.config import Settings
    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
    from qs_ai.transport.grpc.evaluation import EvaluationManagement
    from tests.integration.test_delivery import certificates

    tx, ids, _ = catalog_runs
    before = [await rows(tx, value) for value in ids]
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
    channels = []

    def client(name):
        channel = grpc.aio.secure_channel(
            f"localhost:{port}",
            grpc.ssl_channel_credentials(
                ca, (tmp_path / f"{name}.key").read_bytes(), (tmp_path / f"{name}.pem").read_bytes()
            ),
        )
        channels.append(channel)
        return rpc.EvaluationManagementStub(channel)

    try:
        trusted = client("qs")
        request = pb.EvaluationCatalogQuery(
            scope=pb.PublicationScope(organization_id=1, operator_user_id=42), limit=2
        )
        page = await trusted.List(request, timeout=5)
        assert [item.run_id for item in page.items] == [str(ids[2]), str(ids[1])]
        assert all(
            item.required_candidates == 35 and item.accepted_candidates == 0 for item in page.items
        )
        assert all(item.last_cause == "evaluation_requested" for item in page.items)
        request.cursor = page.next_cursor
        assert len((await trusted.List(request, timeout=5)).items) == 1
        request.scope.organization_id = 2
        with pytest.raises(grpc.aio.AioRpcError) as invalid:
            await trusted.List(request, timeout=5)
        assert invalid.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        request.cursor = ""
        assert [item.run_id for item in (await trusted.List(request, timeout=5)).items] == [
            str(ids[3])
        ]
        with pytest.raises(grpc.aio.AioRpcError) as denied:
            await client("other").List(request, timeout=5)
        assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
        for change in ({"limit": 101}, {"status": "invented"}, {"cursor": "x" * 9000}):
            bad = pb.EvaluationCatalogQuery()
            bad.CopyFrom(request)
            for field, value in change.items():
                setattr(bad, field, value)
            with pytest.raises(grpc.aio.AioRpcError) as invalid:
                await trusted.List(bad, timeout=5)
            assert invalid.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        request.scope.operator_user_id = 0
        with pytest.raises(grpc.aio.AioRpcError) as invalid:
            await trusted.List(request, timeout=5)
        assert invalid.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert [await rows(tx, value) for value in ids] == before
    finally:
        for channel in channels:
            await channel.close()
        await server.stop(0)
        await container.close()


async def test_summary_counts_do_not_return_provider_output(catalog_runs):
    from dataclasses import asdict

    tx, ids, catalog = catalog_runs
    async with tx.open() as db:
        for ordinal in (1, 2):
            await db.execute(
                insert(evaluation_generation_completions).values(
                    run_id=str(ids[2]),
                    execution_id=f"execution:{ordinal}",
                    invocation_id=f"invocation:{ordinal}",
                    case_id="case:1",
                    slot_ordinal=ordinal,
                    execution_ordinal=1,
                    candidate_id=f"candidate:{ordinal}",
                    candidate_json={"review_ready": ordinal == 1},
                    evidence_json={},
                    raw_output=b"private-provider-output",
                    normalized_output=b"private-provider-output",
                )
            )
        await db.commit()
    item = (await catalog.list(EvaluationCatalogQuery(1, 42))).items[0]
    assert (item.required_candidates, item.accepted_candidates, item.review_ready_candidates) == (
        35,
        2,
        1,
    )
    assert "private-provider-output" not in json.dumps(asdict(item))


async def test_microsecond_cursor_and_non_utc_session(catalog_runs):
    from sqlalchemy import text

    tx, ids, catalog = catalog_runs
    async with tx.open() as db:
        await db.execute(text("SET time_zone = '+05:00'"))
        raw = (
            await db.execute(
                select(evaluation_runs.c.definition_json).where(
                    evaluation_runs.c.run_id == str(ids[2])
                )
            )
        ).scalar_one()
        definition = json.loads(raw)
        definition["audit"]["created_at"] = (AT + timedelta(microseconds=123456)).isoformat()
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(ids[2]))
            .values(definition_json=json.dumps(definition))
        )
        await db.commit()
    query = EvaluationCatalogQuery(1, 42, limit=1)
    first = await catalog.list(query)
    assert first.items[0].created_at == "2026-09-13T08:00:00.123456+00:00"
    second = await catalog.list(replace(query, cursor=first.next_cursor))
    assert second.items[0].run_id == str(ids[1])
