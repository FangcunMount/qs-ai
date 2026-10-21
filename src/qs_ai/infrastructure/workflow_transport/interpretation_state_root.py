"""One guarded original interpretation root; no command fabrication or business retry."""

import math
from collections.abc import Awaitable, Callable

from reliable_messaging.transaction import BindingError, Execution, FrozenPlan, LocalOutcome
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import (
    ArchiveGuard,
    TransactionArchive,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_plan import (
    FrozenInterpretationStateEvents,
    InterpretationStateFacts,
    InterpretationStateIntent,
    read_interpretation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import commit_original

StateWrite = Callable[[AsyncSession, InterpretationStateFacts], Awaitable[None]]


class InterpretationStateRoot:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        intent: InterpretationStateIntent,
        winner: FrozenPlan,
        business: StateWrite,
        *,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive interpretation database budget required")
        self._facts, self._archive = read_interpretation_state_plan(intent, winner)
        self._events = FrozenInterpretationStateEvents(intent, winner)
        self._transactions, self._archives, self._store = transactions, archives, store
        self._business, self._database_seconds = business, database_seconds
        self._digest, self._used = winner.digest, False

    async def __call__(self, execution: Execution) -> LocalOutcome:
        if self._used or not execution.owned or execution.plan.digest != self._digest:
            raise BindingError("One owned original interpretation state execution required")
        self._used = True

        async def write(db: AsyncSession, guard: ArchiveGuard) -> None:
            original = db.get_transaction()
            db.info["state_events"] = self._events
            await self._business(db, self._facts)
            if (
                original is None
                or not original.is_active
                or db.get_transaction() is not original
                or db.info.get("state_events") is not self._events
            ):
                raise BindingError(
                    "Interpretation business replaced its original root or transcription"
                )
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
            isolation_level="REPEATABLE READ",
        )
