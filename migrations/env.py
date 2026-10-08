import asyncio

import sqlalchemy as sa
from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.messaging import metadata as messaging_metadata
from qs_ai.infrastructure.persistence.mysql.schema import metadata
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences

settings = Settings()
provided_connection = context.config.attributes.get("connection")
if settings.database_url is None and provided_connection is None:
    raise RuntimeError("QS_AI_DATABASE_URL is required for migrations")
database_url = settings.database_url.get_secret_value() if settings.database_url else None
owned_metadata = sa.MetaData()
for source in (metadata, messaging_metadata):
    for table in source.tables.values():
        table.to_metadata(owned_metadata)
assert evaluation_sequences.name in owned_metadata.tables


def migrate(connection: Connection) -> None:
    context.configure(
        connection=connection,
        version_table_schema=str(connection.scalar(sa.text("SELECT DATABASE()"))),
        target_metadata=owned_metadata,
        compare_type=True,
        include_name=lambda name, kind, parents: kind != "table" or name != "alembic_version",
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


if provided_connection is not None:
    migrate(provided_connection)
elif context.is_offline_mode():
    raise RuntimeError("Data consolidation requires an online isolated database")
else:
    asyncio.run(online())
