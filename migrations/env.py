import asyncio

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.schema import metadata

settings = Settings()
if settings.database_url is None:
    raise RuntimeError("QS_AI_DATABASE_URL is required for migrations")
database_url = settings.database_url.get_secret_value()


def migrate(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=metadata,
        compare_type=True,
        include_name=lambda name, kind, parents: kind != "table" or name in metadata.tables,
    )
    with context.begin_transaction():
        context.run_migrations()


async def online() -> None:
    engine = create_async_engine(database_url, poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(migrate)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(url=database_url, target_metadata=metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(online())
