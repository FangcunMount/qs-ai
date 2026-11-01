"""Actual current-layout assertion; never substitutes an empty query result."""

from sqlalchemy import text


async def assert_retired_publisher_absent(resource):
    async def check(db):
        assert (
            await db.scalar(text("SELECT version_num FROM alembic_version"))
            == "0044_empty_publisher"
        )
        assert (
            await db.scalar(
                text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='messaging_outbox'"
                )
            )
            == 0
        )

    if hasattr(resource, "execute"):
        await check(resource)
    else:
        async with resource.open() as db:
            await check(db)
