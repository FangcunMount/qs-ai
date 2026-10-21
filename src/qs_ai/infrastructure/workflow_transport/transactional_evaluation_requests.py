"""Create-once SDK orchestration; public Run identity retains original replay semantics."""

import asyncio
import hashlib
import json
import math
from dataclasses import asdict
from datetime import datetime
from typing import Any

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
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.infrastructure.persistence.mysql.evaluation_creation_receipt import creation_receipt
from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_input import input_conflicts
from qs_ai.infrastructure.workflow_transport.creation_state import (
    CreationStatePlanner,
    CreationStateRoot,
)
from qs_ai.infrastructure.workflow_transport.creation_state_plan import (
    CreationPlanFacts,
    CreationStateInput,
    read_creation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import EvaluationStateFacts
from qs_ai.infrastructure.workflow_transport.transaction_root import StateTransactions


class CreationTransactionPending(RuntimeError):
    """Reuse the original Run identity or query it; no automatic re-execution."""


class TransactionalEvaluationRequests:
    def __init__(
        self,
        creator: MySQLRunCreator,
        archives: TransactionArchive,
        runtime: StateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive creation evidence budget required")
        self._creator, self._archives, self._runtime = creator, archives, runtime
        self._database_seconds = database_seconds
        self._planner = CreationStatePlanner(
            creator,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )

    async def create(
        self,
        scope: ManagementScope,
        release: EvidenceReleaseIdentity,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        if confirm is not True:
            raise ValueError("Explicit creation confirmation required")
        incoming = CreationStateInput(scope, release, reason)
        EvaluationStateFacts(incoming.intent, at, 0, 0)
        existing = await self._lookup(incoming)
        replay = await self._replayed(incoming, existing)
        if replay is not None:
            return replay
        candidate, creation = await self._planner.candidate(incoming, at)
        try:
            reservation = await self._runtime.reserve(incoming.intent.request, candidate)
        except (BindingError, SchemaError):
            raise
        except ValueError:
            await self._reject_changed_input(incoming)
            raise
        if not reservation.acquired:
            replay = await self._replayed(incoming, reservation)
            if replay is not None:
                return replay
            raise CreationTransactionPending("Original creation execution already owned")
        local = CreationStateRoot(
            self._creator.transactions,
            self._archives,
            self._runtime.store,
            incoming,
            reservation.execution.plan,
            creation,
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
            raise CreationTransactionPending("Original creation root is not confirmed committed")
        await self._verify_commit(incoming, reservation.execution)
        if local.view is None:
            raise CreationTransactionPending("Original creation response unavailable")
        # A lost final Broker confirmation does not undo the local commit or
        # authorize another create. Return this root's captured original response.
        return local.view

    @staticmethod
    def _raise_business(error: Exception | None) -> None:
        if isinstance(error, IntegrityError):
            if error.orig is not None and error.orig.args and error.orig.args[0] == 1062:
                # A competing legacy fact is never adopted as a new committed witness.
                raise CheckpointConflict("Creation request conflicts") from None
        if error is not None:
            raise error

    async def _lookup(self, incoming: CreationStateInput) -> Reservation | None:
        try:
            async with asyncio.timeout(self._database_seconds):
                return await self._runtime.store.lookup(incoming.intent.request)
        except (BindingError, SchemaError):
            raise
        except ValueError:
            await self._reject_changed_input(incoming)
            raise

    async def _reject_changed_input(self, incoming: CreationStateInput) -> None:
        async with asyncio.timeout(self._database_seconds):
            conflict = await input_conflicts(
                self._creator.transactions, self._runtime.store.binding, incoming.intent.request
            )
        if conflict:
            raise CheckpointConflict("Creation Run ID reused with different intent") from None

    async def _replayed(
        self, incoming: CreationStateInput, existing: Reservation | None
    ) -> EvaluationView | None:
        if existing is not None:
            if existing.state != State.COMMITTED:
                raise CreationTransactionPending("Original creation execution unresolved")
            original = await self._verify_commit(incoming, existing.execution)
            view = await self._receipt(incoming, original)
            if view is None:
                raise CreationTransactionPending("Original creation business receipt unavailable")
            return view
        view = await self._receipt(incoming)
        if view is None:
            return None
        # Close the lookup/read race with one fresh boundary, not an unbounded
        # retry loop or a new execution adopting a historical business receipt.
        current = await self._lookup(incoming)
        if current is None:
            return view
        if current.state != State.COMMITTED:
            raise CreationTransactionPending("Original creation execution unresolved")
        original = await self._verify_commit(incoming, current.execution)
        committed = await self._receipt(incoming, original)
        if committed is None:
            raise CreationTransactionPending("Original creation business receipt unavailable")
        return committed

    async def _receipt(
        self, incoming: CreationStateInput, original: CreationPlanFacts | None = None
    ) -> EvaluationView | None:
        async with asyncio.timeout(self._database_seconds), self._creator.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            row = (
                (
                    await db.execute(
                        select(evaluation_runs).where(
                            evaluation_runs.c.run_id == str(incoming.scope.run_id)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            if row["organization_id"] != incoming.scope.organization_id:
                raise CheckpointConflict("Creation request conflicts")
            receipt = json.loads(creation_receipt(dict(row)))
            if (
                receipt["release"] != asdict(incoming.release)
                or receipt["requested_by"] != incoming.scope.actor
                or receipt["request_reason"] != incoming.reason
            ):
                raise CheckpointConflict("Creation request conflicts")
            if original is not None and (
                hashlib.sha256(row["definition_json"].encode()).digest()
                != original.definition_digest
                or row["execution_mode"] != original.execution_mode
            ):
                raise CreationTransactionPending(
                    "Committed creation facts contradict original witness"
                )
            return await read_view(db, incoming.scope)

    async def _verify_commit(
        self, incoming: CreationStateInput, execution: Execution
    ) -> CreationPlanFacts:
        _, original, first = read_creation_state_plan(incoming, execution.plan)
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple(
                [await self._runtime.store.evidence(ref) for ref in execution.references]
            )
            if decide_local(execution.references, evidence) != LocalOutcome.COMMITTED:
                raise CreationTransactionPending("Exact original creation witness unavailable")
            if await self._archives.read_committed(first.envelope().message_id) != first:
                raise CreationTransactionPending("Exact original creation archive unavailable")
        return original
