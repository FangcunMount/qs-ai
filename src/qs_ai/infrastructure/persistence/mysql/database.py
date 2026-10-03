import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol

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


class TransactionEvents(Protocol):
    async def flush(self, db: AsyncSession) -> None: ...


class EventSession(AsyncSession):
    """Flush host state events before the explicit root commit, never after it."""

    async def commit(self) -> None:
        recorder = self.info.get("state_events")
        if recorder is not None and self.in_transaction():
            await recorder.flush(self)
        await super().commit()
        self.info.pop("evaluation_events", None)

    async def rollback(self) -> None:
        try:
            await super().rollback()
        finally:
            self.info.pop("evaluation_events", None)


def changed_evaluation(db: AsyncSession, run_id: str) -> None:
    if db.info.get("state_events") is not None:
        db.info.setdefault("evaluation_events", set()).add(run_id)


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
        self.state_events: TransactionEvents | None = None
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

    async def commit(self, session: AsyncSession) -> None:
        await session.commit()

    @staticmethod
    def borrowed(session: AsyncSession) -> "BorrowedTransactions":
        return BorrowedTransactions(session)

    @asynccontextmanager
    async def open(self) -> AsyncIterator[AsyncSession]:
        if self.database.engine is None:
            raise DependencyUnavailable("Database is not configured")
        factory = async_sessionmaker(
            self.database.engine, class_=EventSession, expire_on_commit=False
        )
        async with factory() as session:
            if self.database.state_events is not None:
                session.info["state_events"] = self.database.state_events
            try:
                yield session
            finally:
                # Uncommitted work is never implicitly accepted, including on cancellation.
                await session.rollback()


class BorrowedTransactions(Transactions):
    """Reuse the caller's root transaction, without owning commit/rollback/close."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.transaction = session.get_transaction()
        self._validate()

    def _validate(self) -> None:
        if (
            self.transaction is None
            or not self.transaction.is_active
            or self.session.get_transaction() is not self.transaction
        ):
            raise RuntimeError("Original active transaction required")

    @asynccontextmanager
    async def open(self) -> AsyncIterator[AsyncSession]:
        self._validate()
        yield self.session
        self._validate()

    async def commit(self, session: AsyncSession) -> None:
        if session is not self.session:
            raise RuntimeError("Cannot complete a different borrowed session")
        self._validate()
        # Only the transport receiver commits business + Inbox + first receipt.
