"""Upgrade the populated lease table, preserving the permanent fencing identity."""

import importlib.util
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


async def test_upgrade_and_downgrade_preserve_execution_fence_and_expiry():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL with migrations applied")
    engine = create_async_engine(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    spec = importlib.util.spec_from_file_location(
        "lease_migration", Path("migrations/versions/0028_execution_leases.py")
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    thread = str(uuid4())
    expected = (thread, 123, datetime(2030, 1, 2, 3, 4, 5, 678901))
    current_table = "execution_leases"

    def apply(connection, action):
        with Operations.context(MigrationContext.configure(connection)):
            action()

    try:
        async with engine.connect() as db:
            try:
                await db.execute(
                    text("INSERT INTO execution_leases VALUES (:id, :fence, :expires)"),
                    {"id": thread, "fence": expected[1], "expires": expected[2]},
                )
                await db.commit()
                await db.run_sync(apply, migration.downgrade)
                current_table = "checkpoint_leases"
                await db.run_sync(apply, migration.upgrade)
                current_table = "execution_leases"
                row = (
                    await db.execute(
                        text(
                            "SELECT thread_id, fence, expires_at FROM execution_leases "
                            "WHERE thread_id=:id"
                        ),
                        {"id": thread},
                    )
                ).one()
                assert tuple(row) == expected
                assert (
                    await db.scalar(
                        text(
                            "SELECT COUNT(*) FROM information_schema.tables "
                            "WHERE table_schema=DATABASE() AND table_name='checkpoint_leases'"
                        )
                    )
                    == 0
                )
            finally:
                if current_table == "checkpoint_leases":
                    await db.run_sync(apply, migration.upgrade)
                await db.execute(
                    text("DELETE FROM execution_leases WHERE thread_id=:id"), {"id": thread}
                )
                await db.commit()
    finally:
        await engine.dispose()
