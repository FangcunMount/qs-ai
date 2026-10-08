"""Read-only stored-fact inventory; synthetic evidence is never a resend permit."""

import json
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions as evaluation_generation_completions,
)
from qs_ai.infrastructure.persistence.mysql.messaging import inbox
from qs_ai.infrastructure.persistence.mysql.schema import (
    model_calls,
    result_outbox,
    runs,
)
from qs_ai.maintenance.messaging_audit import inventory
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import mq_env as mq_env
from tests.integration.test_mq_admission import saved
from tests.integration.test_mq_legacy_handoff import legacy as legacy
from tests.integration.test_mq_storage import keys as keys

pytestmark = pytest.mark.integration


async def test_messaging_inventory_is_read_only_and_borrowed_pool_survives(legacy, monkeypatch):
    tx, event_id, _recorder, _keys = legacy
    before = await saved(tx, result_outbox)
    execute = AsyncSession.execute
    checked = []

    async def enforce_read_only(db, statement, *args, **kwargs):
        value = await execute(db, statement, *args, **kwargs)
        if str(statement) == "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY":
            with pytest.raises(DBAPIError) as error:
                await execute(db, text(f"UPDATE {result_outbox.name} SET attempts=7"))
            assert error.value.orig.args[0] == 1792
            checked.append(True)
        return value

    monkeypatch.setattr(AsyncSession, "execute", enforce_read_only)
    report = await inventory(tx)
    assert checked == [True] and report["complete"]
    assert report["handoff_schema_compatible"]
    assert any(r["event_id"] == event_id for r in report["sections"]["result_outbox"]["rows"])
    assert await saved(tx, result_outbox) == before
    async with tx.open() as db:
        assert (
            await db.scalar(
                select(result_outbox.c.event_id).where(result_outbox.c.event_id == event_id)
            )
            == event_id
        )


async def test_messaging_inventory_missing_table_is_unavailable_not_empty(legacy):
    tx, *_ = legacy
    async with tx.open() as db:
        await db.execute(text(f"RENAME TABLE {inbox.name} TO mq_inventory_hidden"))
    try:
        report = await inventory(tx)
        section = report["sections"]["ai_messaging_inbox"]
        assert not report["complete"] and not section["table_exists"]
        assert section["rows"] is None and section["total"] is None
        assert report["schema_heads"] == [NEW_HEAD]
    finally:
        async with tx.open() as db:
            await db.execute(text(f"RENAME TABLE mq_inventory_hidden TO {inbox.name}"))


async def test_messaging_inventory_truncation_never_claims_complete(legacy):
    tx, _id, *_ = legacy
    row = dict((await saved(tx, result_outbox))[0])
    row.update(event_id=str(uuid4()), version=row["version"] + 1)
    row["payload"] = {**row["payload"], "event_id": row["event_id"], "version": row["version"]}
    async with tx.open() as db:
        await db.execute(insert(result_outbox).values(**row))
        await db.commit()
    report = await inventory(tx, max_rows=1)
    section = report["sections"]["result_outbox"]
    assert not report["complete"] and section["truncated"]
    assert section["total"] >= 2 and len(section["rows"]) == 1


async def test_messaging_inventory_keeps_unknown_facts_without_body_or_retry_authorization(legacy):
    tx, *_ = legacy
    secret = "isolated-audit-sensitive-body-never-exported"
    evaluation_id, execution_id, invocation_id = (str(uuid4()) for _ in range(3))
    async with tx.open() as db:
        run_id = await db.scalar(select(runs.c.id).limit(1))
        await db.execute(
            insert(model_calls).values(
                run_id=run_id,
                invocation_id=str(uuid4()),
                fence_token=1,
                status="unknown",
                request_json=secret,
                failure_code="result_unknown",
            )
        )
        await db.execute(
            evaluation_generation_completions.insert().values(
                run_id=evaluation_id,
                execution_id=execution_id,
                invocation_id=invocation_id,
                case_id="inventory-fixture",
                slot_ordinal=1,
                execution_ordinal=1,
                evidence_json={"status": "result_unknown", "provider_call_count": 1},
                raw_output=secret.encode(),
                normalized_output=secret.encode(),
            )
        )
        await db.commit()
    try:
        report = await inventory(tx)
        unknown = next(
            r for r in report["sections"]["model_calls"]["rows"] if r["run_id"] == run_id
        )
        assert unknown["status"] == "unknown" and not unknown["persistent_response_present"]
        completion = next(
            r
            for r in report["sections"]["evaluation_generation_completions"]["rows"]
            if r["execution_id"] == execution_id
        )
        assert completion["original_status"] == "result_unknown"
        assert "no recovery or model retry authorization" in report["classification"]
        assert secret not in json.dumps(report)
    finally:
        async with tx.open() as db:
            await db.execute(
                evaluation_generation_completions.delete().where(
                    evaluation_generation_completions.c.run_id == evaluation_id
                )
            )
            await db.commit()
