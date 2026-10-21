"""Explicit no-event quota deferral after exact aborted publication proof."""

import asyncio
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import BindingError, Execution, LocalOutcome, decide_local
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.persistence.mysql.database import BorrowedTransactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore, expiry
from qs_ai.infrastructure.persistence.mysql.participant_capacity import acquire
from qs_ai.infrastructure.persistence.mysql.schema import jobs
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_schema import binding
from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import (
    InterpretationClaimSource,
    read_claim_plan,
    read_claim_source,
)


class InterpretationDeferralPending(RuntimeError):
    """No model permission or message decision; inspect the existing operational job."""


class _JobOnlyEvents:
    """One operational root cannot transcribe or flush any business state event."""

    def __init__(self, db: AsyncSession) -> None:
        root = db.get_transaction()
        if root is None or not root.is_active or db.info.get("evaluation_events"):
            raise BindingError("Clean independent job-only root required")
        self._db, self._root = db, root

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        raise BindingError("Job-only maintenance cannot write interpretation state")

    async def flush(self, db: AsyncSession) -> None:
        if (
            db is not self._db
            or db.get_transaction() is not self._root
            or not self._root.is_active
            or db.info.get("state_events") is not self
            or db.info.get("evaluation_events")
        ):
            raise BindingError("Job-only maintenance root or event boundary changed")


class InterpretationClaimDeferral:
    def __init__(
        self,
        business: MySQLExecutionStore,
        store: MySQLTransactionStore,
        archives: TransactionArchive,
        *,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive original deferral budget required")
        if isinstance(business.transactions, BorrowedTransactions):
            raise BindingError("Independent job maintenance cannot borrow a business root")
        if store.binding.owner != "ai.mysql":
            raise BindingError("Original ai.mysql deferral binding required")
        self._business, self._store, self._archives = business, store, archives
        self._database_seconds = database_seconds

    async def __call__(
        self,
        source: InterpretationClaimSource,
        ttl_seconds: int,
        previous: Execution,
    ) -> bool:
        """Only delay the same still-capacity-denied job; never call its claim executor."""
        _, first = read_claim_plan(source, ttl_seconds, previous.plan)
        if previous.writer_epoch != self._store.binding.writer_epoch:
            raise BindingError("Original deferral writer epoch differs")
        if source.exhausted or not source.session.uses_qs_snapshot:
            return False
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple([await self._store.evidence(ref) for ref in previous.references])
            if decide_local(previous.references, evidence) != LocalOutcome.ABORTED:
                raise InterpretationDeferralPending("Exact previous claim abort proof required")
            if await self._archives.read_committed(first.envelope().message_id) is not None:
                raise InterpretationDeferralPending("Previous claim archive contradicts abort")
            # An independent operational action, not a repeated SDK local executor.
            # First clock is outside its root; the original capacity ledger retains
            # daily reservation semantics. There is no new message or model lease.
            at = datetime.now(UTC)
            async with self._business.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                root = db.get_transaction()
                events = _JobOnlyEvents(db)
                db.info["state_events"] = events
                appender = bind(db)
                await appender.validate()
                installed = (
                    (
                        await db.execute(
                            select(binding)
                            .where(
                                binding.c.owner == self._store.binding.owner.encode(),
                            )
                            .with_for_update(read=True)
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    installed is None
                    or installed["store_id"] != self._store.binding.store_id.encode()
                    or installed["writer_epoch"] != self._store.binding.writer_epoch.encode()
                    or installed["schema_version"] != 1
                ):
                    raise BindingError("Original deferral database binding differs")
                try:
                    current = await read_claim_source(
                        self._business,
                        db,
                        source.job_id,
                        source.session.id,
                        lock=True,
                    )
                except LeaseLost:
                    return False
                if current != source:
                    return False
                if await acquire(
                    db,
                    current.session,
                    self._business.capacity,
                    at,
                    self._business.quota_baseline,
                ):
                    return False  # Open() rolls back any staged slot; it cannot grant a claim.
                await db.execute(
                    update(jobs)
                    .where(jobs.c.id == source.job_id)
                    .values(
                        available_at=expiry(1),
                    )
                )
                await appender.validate()
                if root is None or not root.is_active or db.get_transaction() is not root:
                    raise InterpretationDeferralPending("Original deferral root replaced")
                await events.flush(db)
                try:
                    await db.commit()
                except (Exception, asyncio.CancelledError):
                    # The job-only commit may have happened; never report rollback,
                    # replay a model, or claim that a message was committed.
                    raise InterpretationDeferralPending(
                        "Original deferral commit unresolved"
                    ) from None
        return True
