"""Original live session/job/lease rules exposed read-only; not dispatch/commit authority."""

import pytest
from sqlalchemy import event, text, update

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.persistence.mysql.schema import execution_leases
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_interpretation_finish_root import snapshot
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration


async def claim(kit):
    await kit.queued()
    value = await kit.store.claim(60)
    assert value is not None
    return value


async def test_original_claim_source_only_selects_without_locks_or_business_mutation(kit):
    value = await claim(kit)
    before, statements = await snapshot(kit, value), []

    def record(_connection, _cursor, statement, *_):
        statements.append(statement.strip().upper())

    engine = kit.transactions.database.engine.sync_engine
    async with kit.transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        event.listen(engine, "before_cursor_execute", record)
        try:
            session = await kit.store.read_claim(db, value)
        finally:
            event.remove(engine, "before_cursor_execute", record)
    assert session == value.session
    assert len(statements) == 3 and all(s.startswith("SELECT ") for s in statements)
    assert not any("FOR UPDATE" in s for s in statements)
    assert await snapshot(kit, value) == before


@pytest.mark.parametrize("readonly", [False, True])
async def test_expired_lease_cannot_become_execution_authority_from_original_or_readonly_guard(
    kit, readonly
):
    value = await claim(kit)
    async with kit.transactions.open() as db:
        await db.execute(
            update(execution_leases)
            .where(execution_leases.c.thread_id == value.session.id)
            .values(expires_at=text("TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6))"))
        )
        await db.commit()
    before = await snapshot(kit, value)
    async with kit.transactions.open() as db:
        with pytest.raises(LeaseLost):
            if readonly:
                await kit.store.read_claim(db, value)
            else:
                await kit.store._guard(db, value)
    assert await snapshot(kit, value) == before


async def test_readonly_hint_does_not_hold_locks_against_original_lease_renewal(kit):
    value = await claim(kit)
    async with kit.transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        assert await kit.store.read_claim(db, value) == value.session
        await kit.store.renew(value, 60)
    async with kit.transactions.open() as db:
        assert await kit.store._guard(db, value) == value.session
