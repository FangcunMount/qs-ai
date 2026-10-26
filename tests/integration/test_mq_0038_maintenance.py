"""Known historical layout still supports reviewed maintenance on real MySQL."""

import os
from dataclasses import asdict
from datetime import datetime
from uuid import uuid4

import pytest
import sqlalchemy as sa

from qs_ai.application.integration.events import StateEvent
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.maintenance.messaging_audit import inventory
from qs_ai.maintenance.schema_layout import handoff_tables
from qs_ai.maintenance.schema_refactor.layouts import OLD_HEAD
from qs_ai.maintenance.schema_refactor.v0038 import metadata

pytestmark = pytest.mark.integration


@pytest.fixture
async def old_storage():
    url = os.environ.get("QS_MQ_MYSQL_URL")
    if not url:
        pytest.fail("Required disposable QS_MQ_MYSQL_URL is missing")
    database = Database(url)
    assert database.engine is not None
    version_metadata = sa.MetaData()
    version = sa.Table(
        "alembic_version",
        version_metadata,
        sa.Column("version_num", sa.String(32), primary_key=True),
    )
    async with database.engine.begin() as db:
        count = await db.scalar(
            sa.text("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()")
        )
        assert count == 0, "Dedicated disposable adapter database must be empty"
        await db.run_sync(metadata.create_all)
        await db.run_sync(version_metadata.create_all)
        await db.execute(sa.insert(version).values(version_num=OLD_HEAD))
    try:
        yield database, Transactions(database), version
    finally:
        async with database.engine.begin() as db:
            await db.run_sync(metadata.drop_all)
            await db.run_sync(version_metadata.drop_all)
        await database.close()


async def test_retained_0038_inventory_does_not_change_original_rows(old_storage):
    _, transactions, _ = old_storage
    result, mq_outbox = handoff_tables(OLD_HEAD)
    sessions = metadata.tables["interpretation_sessions"]
    event_id, session_id, request_id = (str(uuid4()) for _ in range(3))
    at = datetime(2026, 9, 22, 11, 22, 33, 444555)
    event = StateEvent(event_id, request_id, session_id, Actor("7", "legacy"), "7", 1, "created")
    async with transactions.open() as db:
        await db.execute(
            sa.insert(sessions).values(
                id=session_id,
                org_id=7,
                owner_subject_id="legacy",
                testee_id=7,
                assessment_ids=["42"],
                goal="旧布局 Unicode 🙂",
                status="created",
                version=1,
                workflow_version=1,
                created_at=at,
                updated_at=at,
            )
        )
        await db.execute(
            sa.insert(result).values(
                event_id=event_id,
                session_id=session_id,
                version=1,
                payload=asdict(event),
                attempts=8,
                available_at=at,
                created_at=None,
            )
        )
        await db.commit()
    report = await inventory(transactions)
    assert report["complete"] and report["schema_heads"] == [OLD_HEAD]
    assert report["sections"]["result_outbox"]["physical_table"] == "result_outbox"
    assert report["sections"]["ai_messaging_outbox"]["total"] == 0
    async with transactions.open() as db:
        source = (await db.execute(sa.select(result))).mappings().one()
        assert source["attempts"] == 8 and source["created_at"] is None and not source["mq_owned"]
    assert report["sections"]["evaluation_generation_completions"]["physical_table"] == (
        "evaluation_generation_completions"
    )


async def test_unrecognized_head_refuses_old_audit_and_handoff_before_any_write(old_storage):
    _, transactions, version = old_storage
    async with transactions.open() as db:
        await db.execute(sa.update(version).values(version_num="0039_data_consolidation"))
        await db.commit()
    with pytest.raises(ValueError, match="recognized complete schema"):
        await inventory(transactions)
    result, mq_outbox = handoff_tables(OLD_HEAD)
    async with transactions.open() as db:
        assert await db.scalar(sa.select(sa.func.count()).select_from(result)) == 0
        assert await db.scalar(sa.select(sa.func.count()).select_from(mq_outbox)) == 0
