"""UTC migration must not guess the timezone of historical wall clocks."""

import importlib.util
from datetime import datetime
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from tests.integration.test_interpretation import kit as kit

pytestmark = pytest.mark.integration


async def test_additive_migration_preserves_old_values_and_leaves_utc_unknown(kit):
    spec = importlib.util.spec_from_file_location(
        "utc_migration", Path("migrations/versions/0035_runtime_utc_timestamps.py")
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def upgrade(connection):
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()

    # Connection-local temporary tables shadow real tables; shared schema is untouched.
    engine = create_async_engine(
        kit.dsn.replace("mysql://", "mysql+asyncmy://", 1), poolclass=NullPool
    )
    try:
        async with engine.connect() as db:
            for table in ("interpretation_sessions", "model_calls"):
                await db.execute(
                    text(
                        f"CREATE TEMPORARY TABLE {table} "
                        "(id INT PRIMARY KEY, created_at DATETIME(6))"
                    )
                )
                await db.execute(
                    text(f"INSERT INTO {table} VALUES (1, '2026-09-14 23:02:28.123456')")
                )
            await db.run_sync(upgrade)
            for table in ("interpretation_sessions", "model_calls"):
                row = (
                    await db.execute(text(f"SELECT created_at,created_at_utc FROM {table}"))
                ).one()
                assert row == (datetime(2026, 9, 14, 23, 2, 28, 123456), None)
            assert (
                await db.scalar(text("SELECT updated_at_utc FROM interpretation_sessions")) is None
            )
            with pytest.raises(RuntimeError, match="Preserve UTC evidence"):
                migration.downgrade()
    finally:
        await engine.dispose()
