"""Original Prepare command orchestration; no business retry or legacy witness adoption."""

import asyncio
import math
from datetime import datetime
from typing import Any

from reliable_messaging.durable import MessageConflict
from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    BindingError,
    Execution,
    LocalOutcome,
    Reservation,
    SchemaError,
    State,
    decide_local,
)
from sqlalchemy.exc import IntegrityError

from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_input import input_conflicts
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationFacts,
    SolutionPreparationInput,
    read_preparation_plan,
)
from qs_ai.infrastructure.workflow_transport.solution_preparation_state import (
    SolutionPreparationPlanner,
    SolutionPreparationRoot,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import StateTransactions


class PreparationTransactionPending(RuntimeError):
    """Read the original command receipt; no automatic Retry/Replan is authorized."""


class SolutionPreparationDispatcher:
    def __init__(
        self,
        original: MySQLSolutions,
        archives: TransactionArchive,
        runtime: StateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive preparation evidence budget required")
        self._original, self._archives, self._runtime = original, archives, runtime
        self._database_seconds = database_seconds
        self._planner = SolutionPreparationPlanner(
            original,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def __call__(self, incoming: SolutionPreparationInput, at: datetime) -> dict[str, Any]:
        EvaluationStateFacts(incoming.intent, at, 0, 0)
        # Preserve the original receipt's organization/actor/request protection
        # before classifying protocol input conflicts. This is a business read,
        # never a substitute for exact SDK evidence.
        replay = await self._planner.replay(incoming)
        existing = await self._lookup(incoming)
        recovered = await self._recovered(incoming, existing, replay)
        if recovered is not None:
            return recovered
        candidate = await self._planner.candidate(incoming, at)
        if candidate is None:
            recovered = await self._recovered(
                incoming, await self._lookup(incoming), await self._planner.replay(incoming)
            )
            if recovered is not None:
                return recovered
            raise PreparationTransactionPending("Original preparation receipt unavailable")
        try:
            reservation = await self._runtime.reserve(incoming.intent.request, candidate.winner)
        except (BindingError, SchemaError):
            raise
        except ValueError as error:
            await self._reject_changed_input(incoming)
            raise PreparationTransactionPending(
                "Original preparation reservation unresolved"
            ) from error
        if not reservation.acquired:
            recovered = await self._recovered(
                incoming, reservation, await self._planner.replay(incoming)
            )
            if recovered is not None:
                return recovered
            raise PreparationTransactionPending("Original preparation execution already owned")
        local = SolutionPreparationRoot(
            self._original,
            self._archives,
            self._runtime.store,
            incoming,
            reservation.execution.plan,
            candidate,
            database_seconds=self._database_seconds,
        )
        try:
            result = await self._runtime.execute(reservation.execution, local)
        except TransactionSendError as error:
            result = error.result
            if result.local == LocalOutcome.ABORTED:
                self._raise_business(local.business_error)
            if result.local != LocalOutcome.COMMITTED:
                raise
        if result.local != LocalOutcome.COMMITTED:
            if result.local == LocalOutcome.ABORTED:
                self._raise_business(local.business_error)
            raise PreparationTransactionPending("Original preparation root not confirmed committed")
        await self._verify_commit(incoming, reservation.execution)
        if local.value is None:
            raise PreparationTransactionPending("Original preparation response unavailable")
        # The captured response becomes usable only with fresh exact proof;
        # LocalCommitted + FinalUnknown retains unresolved Broker responsibility.
        return local.value

    @staticmethod
    def _raise_business(error: Exception | None) -> None:
        if isinstance(error, IntegrityError):
            if error.orig is not None and error.orig.args and error.orig.args[0] == 1062:
                raise DraftConflict("Solution identity or immutable asset already exists") from None
        if error is not None:
            raise error

    async def _lookup(self, incoming: SolutionPreparationInput) -> Reservation | None:
        try:
            async with asyncio.timeout(self._database_seconds):
                return await self._runtime.store.lookup(incoming.intent.request)
        except (BindingError, SchemaError):
            raise
        except ValueError as error:
            await self._reject_changed_input(incoming)
            raise PreparationTransactionPending(
                "Original preparation protocol unreadable"
            ) from error

    async def _reject_changed_input(self, incoming: SolutionPreparationInput) -> None:
        async with asyncio.timeout(self._database_seconds):
            conflict = await input_conflicts(
                self._original.transactions, self._runtime.store.binding, incoming.intent.request
            )
        if conflict:
            raise DraftConflict(
                "Preparation command identity reused with different intent"
            ) from None

    async def _recovered(
        self,
        incoming: SolutionPreparationInput,
        existing: Reservation | None,
        replay: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if existing is None:
            if replay is None:
                return None
            # Close the receipt/lookup race once; do not create another plan or
            # adopt a legacy receipt as a new SDK witness.
            existing = await self._lookup(incoming)
            if existing is None:
                return replay
        if existing.state != State.COMMITTED:
            raise PreparationTransactionPending("Original preparation execution is unresolved")
        facts = await self._verify_commit(incoming, existing.execution)
        try:
            checked = await self._planner.replay(incoming, expected_target=facts.target_digest)
        except MessageConflict as error:
            raise PreparationTransactionPending(
                "Committed preparation receipt contradicts first plan"
            ) from error
        if checked is None:
            raise PreparationTransactionPending("Original preparation business receipt unavailable")
        return checked

    async def _verify_commit(
        self,
        incoming: SolutionPreparationInput,
        execution: Execution,
    ) -> SolutionPreparationFacts:
        _, facts, first = read_preparation_plan(incoming, execution.plan)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise PreparationTransactionPending(
                    "Exact original preparation witness unavailable"
                )
            if await self._archives.read_committed(first.envelope().message_id) != first:
                raise PreparationTransactionPending("Exact first preparation archive unavailable")
        return facts
