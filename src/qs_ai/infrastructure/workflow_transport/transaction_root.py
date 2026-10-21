"""Shared original-root commit boundary; no planning, Seal, PUB or business retry."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Literal, Protocol

from reliable_messaging.transaction import (
    BindingError,
    Execution,
    FrozenPlan,
    LocalExecutor,
    LocalOutcome,
    ReplanAuthorization,
    Request,
    Reservation,
    Result,
    RetryAuthorization,
    decide_local,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import (
    ArchiveGuard,
    TransactionArchive,
)


class TransactionAdmissionClosed(RuntimeError):
    """Host admission is closed while transaction checking may remain available."""


class StateTransactions(Protocol):
    @property
    def store(self) -> MySQLTransactionStore: ...

    async def reserve(self, request: Request, candidate: FrozenPlan) -> Reservation: ...

    async def execute(self, execution: Execution, local: LocalExecutor) -> Result: ...


class AuthorizedStateTransactions(StateTransactions, Protocol):
    """Explicit host permission only; admission and owned work remain SDK bounded."""

    async def retry(self, authorization: RetryAuthorization) -> Reservation: ...

    async def replan(self, authorization: ReplanAuthorization) -> Reservation: ...

    def stats(self) -> dict[str, int | bool]: ...


OriginalWrite = Callable[[AsyncSession, ArchiveGuard], Awaitable[None]]


async def commit_original(
    transactions: Transactions,
    archives: TransactionArchive,
    store: MySQLTransactionStore,
    execution: Execution,
    write: OriginalWrite,
    *,
    database_seconds: float,
    isolation_level: Literal["READ COMMITTED", "REPEATABLE READ"] | None = None,
) -> LocalOutcome:
    """Invoke one already frozen write on the host root, then read fresh proof if needed.

    The SDK prepares halves before calling its local executor. That executor
    owns its one-use admission and plan validation; this boundary never retries
    it. write stages business and exact archives, without owning the root.
    """
    if not execution.owned:
        raise BindingError("Owned execution required for original root")
    committing, rolled_back, integrity_lost = False, False, False
    try:
        async with transactions.open() as db:
            await db.begin()
            original = db.get_transaction()
            try:
                if isolation_level is not None:
                    await db.connection(execution_options={"isolation_level": isolation_level})
                # Competes with the same execution's protocol aborter before business SQL.
                guard = await archives.bind(db, execution)
                await write(db, guard)
                if (
                    original is None
                    or not original.is_active
                    or db.get_transaction() is not original
                ):
                    integrity_lost = True
                    raise BindingError("Business replaced the original root")
                await guard.commit_witness()
                committing = True
                await db.commit()
                return LocalOutcome.COMMITTED
            except (Exception, asyncio.CancelledError):
                if not committing:
                    if (
                        original is None
                        or not original.is_active
                        or db.get_transaction() is not original
                    ):
                        integrity_lost = True
                    if not integrity_lost:
                        await db.rollback()
                        rolled_back = True
                raise
    except (Exception, asyncio.CancelledError):
        if committing or integrity_lost:
            # This follows root cleanup, never the old transaction's read snapshot.
            try:
                async with asyncio.timeout(database_seconds):
                    evidence = tuple([await store.evidence(ref) for ref in execution.references])
                    return decide_local(execution.references, evidence)
            except (Exception, asyncio.CancelledError):
                return LocalOutcome.UNKNOWN
        if rolled_back:
            try:
                async with asyncio.timeout(database_seconds):
                    if await store.abort(execution):
                        return LocalOutcome.ABORTED
            except (Exception, asyncio.CancelledError):
                pass
        return LocalOutcome.UNKNOWN
