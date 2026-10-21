"""One non-command state executor; the host retains business/root ownership."""

import math
from collections.abc import Awaitable, Callable
from typing import Literal

from reliable_messaging.transaction import BindingError, Execution, FrozenPlan, LocalOutcome
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import (
    ArchiveGuard,
    TransactionArchive,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    EvaluationStateIntent,
    read_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.frozen_evaluation_events import (
    FrozenEvaluationStateEvents,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import commit_original

StateWrite = Callable[[AsyncSession, EvaluationStateFacts], Awaitable[None]]


class EvaluationStateRoot:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        intent: EvaluationStateIntent,
        winner: FrozenPlan,
        business: StateWrite,
        *,
        database_seconds: float = 2,
        isolation_level: Literal["READ COMMITTED", "REPEATABLE READ"] | None = None,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive state evidence budget required")
        if isolation_level not in (None, "READ COMMITTED", "REPEATABLE READ"):
            raise ValueError("Explicit supported root isolation required")
        self._isolation_level = isolation_level
        self._facts, self._archive = read_evaluation_state_plan(intent, winner)
        self._events = FrozenEvaluationStateEvents(intent, winner)
        self._transactions, self._archives, self._store = transactions, archives, store
        self._business, self._database_seconds = business, database_seconds
        self._plan_digest, self._used = winner.digest, False

    async def __call__(self, execution: Execution) -> LocalOutcome:
        if self._used or not execution.owned or execution.plan.digest != self._plan_digest:
            raise BindingError("One owned original state execution required")
        self._used = True

        async def write(db: AsyncSession, guard: ArchiveGuard) -> None:
            original = db.get_transaction()
            db.info["state_events"] = self._events
            # Only already frozen winner facts enter the original business adapter.
            await self._business(db, self._facts)
            if (
                original is None
                or not original.is_active
                or db.get_transaction() is not original
                or db.info.get("state_events") is not self._events
            ):
                raise BindingError("State business replaced its original root or transcription")
            await self._events.flush(db)
            db.info.pop("state_events", None)
            await guard.put(execution.plan.branches[0].branch_id)

        return await commit_original(
            self._transactions,
            self._archives,
            self._store,
            execution,
            write,
            database_seconds=self._database_seconds,
            isolation_level=self._isolation_level,
        )
