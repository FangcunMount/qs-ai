"""Database-local wall clocks must never become invented UTC instants."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import event, select, update

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.infrastructure.persistence.mysql.runtime import MySQLRuntimeReader
from qs_ai.infrastructure.persistence.mysql.schema import model_calls, sessions
from tests.integration.test_runtime import bind
from tests.integration.test_runtime import kit as kit

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("zone", ["+08:00", "-05:00", "+00:00"])
async def test_new_times_are_utc_even_when_database_session_is_not(kit, zone):
    engine = kit.transactions.database.engine

    def set_zone(connection, *_):
        cursor = connection.cursor()
        cursor.execute("SET time_zone = '" + zone + "'")
        cursor.close()

    event.listen(engine.sync_engine, "checkout", set_zone)
    try:
        before = datetime.now(UTC)
        receipt, _ = await bind(kit)
        claim = await kit.store.claim(30)
        call, created = await kit.store.begin_model_call(claim, "{}")
        assert created
        result = await MySQLRuntimeReader(kit.transactions).detail(
            DraftScope(1, 42), receipt.session_id
        )
        after = datetime.now(UTC)
        attempt = result["attempts"][0]
        assert attempt["invocation_id"] == call.invocation_id
        assert attempt["model_call_time_basis"] == "utc"
        assert attempt["model_call_created_at_recorded"] is None
        for value in (
            result["execution"]["created_at"],
            result["execution"]["updated_at"],
            attempt["model_call_created_at"],
        ):
            assert before <= datetime.fromisoformat(value) <= after
        # Reading diagnostics cannot dispatch a second call.
        same, created = await kit.store.begin_model_call(claim, "{}")
        assert not created and same.invocation_id == call.invocation_id
    finally:
        event.remove(engine.sync_engine, "checkout", set_zone)


async def test_legacy_wall_clock_is_preserved_without_guessing_its_timezone(kit):
    receipt, _ = await bind(kit)
    claim = await kit.store.claim(30)
    await kit.store.begin_model_call(claim, "{}")
    legacy = datetime(2026, 9, 14, 23, 2, 28)
    async with kit.transactions.open() as db:
        await db.execute(
            update(model_calls)
            .where(model_calls.c.run_id == claim.run_id)
            .values(created_at=legacy, created_at_utc=None)
        )
        await db.execute(
            update(sessions)
            .where(sessions.c.id == receipt.session_id)
            .values(created_at_utc=None, updated_at_utc=None)
        )
        await db.commit()
    result = await MySQLRuntimeReader(kit.transactions).detail(
        DraftScope(1, 42), receipt.session_id
    )
    assert result["execution"]["created_at"] is None
    assert result["execution"]["updated_at"] is None
    attempt = result["attempts"][0]
    assert attempt["model_call_created_at"] is None
    assert attempt["model_call_created_at_recorded"] == "2026-09-14T23:02:28"
    assert attempt["model_call_time_basis"] == "legacy_timezone_unrecorded"
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(model_calls.c.created_at).where(model_calls.c.run_id == claim.run_id)
            )
            == legacy
        )


async def test_old_writer_update_invalidates_stale_utc_marker(kit):
    receipt, _ = await bind(kit)
    async with kit.transactions.open() as db:
        await db.execute(
            update(sessions)
            .where(sessions.c.id == receipt.session_id)
            .values(updated_at=datetime(2030, 1, 1))
        )
        await db.commit()
    result = await MySQLRuntimeReader(kit.transactions).detail(
        DraftScope(1, 42), receipt.session_id
    )
    assert result["execution"]["updated_at"] is None
