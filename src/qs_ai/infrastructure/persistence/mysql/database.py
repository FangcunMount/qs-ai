import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from qs_ai.application.interpretation.ports import DependencyUnavailable
from qs_ai.application.operations.health import Readiness


class Base(DeclarativeBase):
    pass


class Database:
    def __init__(
        self,
        url: str | None,
        *,
        pool_size: int = 5,
        max_overflow: int = 5,
        pool_timeout: float = 3,
        connect_timeout: int = 3,
    ) -> None:
        self.engine: AsyncEngine | None = None
        if url is not None:
            self.engine = create_async_engine(
                url,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                connect_args={"connect_timeout": connect_timeout},
                hide_parameters=True,
            )

    async def close(self) -> None:
        if self.engine is not None:
            await self.engine.dispose()


class MySQLProbe:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def check(self) -> Readiness:
        if self.database.engine is None:
            return Readiness("not_configured")
        try:
            async with asyncio.timeout(5):
                async with self.database.engine.connect() as connection:
                    await connection.execute(text("SELECT 1"))
        except Exception:
            return Readiness("unavailable")
        return Readiness("connected")


class Transactions:
    """Each block owns a fresh session; callers must explicitly commit."""

    def __init__(self, database: Database) -> None:
        self.database = database

    @asynccontextmanager
    async def open(self) -> AsyncIterator[AsyncSession]:
        if self.database.engine is None:
            raise DependencyUnavailable("Database is not configured")
        factory = async_sessionmaker(self.database.engine, expire_on_commit=False)
        async with factory() as session:
            try:
                yield session
            finally:
                # Uncommitted work is never implicitly accepted, including on cancellation.
                await session.rollback()
