"""Explicit one-candidate claim; duplicate committed requests never repeat model handoff."""

import asyncio
import math
from typing import Any

from reliable_messaging.rocketmq_transaction import TransactionSendError
from reliable_messaging.transaction import (
    Execution,
    LocalOutcome,
    RetryAuthorization,
    State,
    decide_local,
)
from sqlalchemy import select

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.schema import jobs
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import (
    InterpretationClaimPlanner,
    InterpretationClaimSource,
    InterpretationClaimWrite,
    read_claim_plan,
)
from qs_ai.infrastructure.workflow_transport.interpretation_state_root import (
    InterpretationStateRoot,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class InterpretationClaimPending(RuntimeError):
    """One original claim remains unresolved; no retry or model permission is granted."""


class InterpretationClaimQuotaDenied(InterpretationClaimPending):
    """Local root is exactly Aborted after original quota denial; no model grant."""


class InterpretationClaimSender:
    def __init__(
        self,
        store: MySQLExecutionStore,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive claim database budget required")
        self._store, self._archives, self._runtime = store, archives, runtime
        self._database_seconds = database_seconds
        self._planner = InterpretationClaimPlanner(
            store,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            route_resolver=runtime.routing,
        )

    async def _committed(
        self,
        source: InterpretationClaimSource,
        ttl_seconds: int,
        owned: Execution,
    ) -> None:
        _, first = read_claim_plan(
            source, ttl_seconds, owned.plan, route_resolver=self._runtime.routing
        )
        async with asyncio.timeout(self._database_seconds):
            evidence = tuple([await self._runtime.store.evidence(ref) for ref in owned.references])
            if decide_local(owned.references, evidence) != LocalOutcome.COMMITTED:
                raise InterpretationClaimPending("Exact original claim witness unavailable")
            if await self._archives.read_committed(first.envelope().message_id) != first:
                raise InterpretationClaimPending("Exact original claim archive unavailable")

    async def _handoff(self, claim: Claim) -> Claim | None:
        # The first owner's model handoff also needs a fresh active original lease.
        # Witness alone remains true after lease expiry or subsequent business work.
        async with asyncio.timeout(self._database_seconds):
            async with self._store.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                try:
                    session = await self._store.read_claim(db, claim)
                except LeaseLost:
                    return None
                job = (
                    (await db.execute(select(jobs).where(jobs.c.id == claim.job_id)))
                    .mappings()
                    .one()
                )
                if (
                    session != claim.session
                    or job["session_id"] != claim.session.id
                    or job["run_id"] != claim.run_id
                    or job["answer"] != claim.answer
                    or job["skipped"] != claim.skipped
                    or job["question_id"] != claim.question_id
                ):
                    return None
        return claim

    async def source(self, job_id: str, session_id: str) -> InterpretationClaimSource:
        return await self._planner.source(job_id, session_id)

    async def __call__(self, source: InterpretationClaimSource, ttl_seconds: int) -> Claim | None:
        intent = source.intent(ttl_seconds)
        async with asyncio.timeout(self._database_seconds):
            reserved = await self._runtime.store.lookup(intent.request)
        if reserved is None:
            winner = await self._planner.candidate(source, ttl_seconds)
            reserved = await self._runtime.reserve(intent.request, winner)
        if not reserved.acquired:
            if reserved.state == State.COMMITTED:
                await self._committed(source, ttl_seconds, reserved.execution)
                return None  # A duplicate is proof lookup, never a second model admission.
            raise InterpretationClaimPending("Original claim already owned or unresolved")
        return await self._execute_owned(source, ttl_seconds, reserved.execution)

    async def retry(
        self,
        source: InterpretationClaimSource,
        ttl_seconds: int,
        previous: Execution,
    ) -> Claim | None:
        """An explicit host decision, never called by Checker or ordinary duplicate lookup."""
        read_claim_plan(source, ttl_seconds, previous.plan, route_resolver=self._runtime.routing)
        current = await self._planner.source(source.job_id, source.session.id)
        if current != source:
            raise LeaseLost("Original claim retry source differs from its frozen plan")
        reserved = await self._runtime.retry(RetryAuthorization(previous))
        if not reserved.acquired:
            raise InterpretationClaimPending("Explicit claim retry was not acquired")
        return await self._execute_owned(source, ttl_seconds, reserved.execution)

    async def _execute_owned(
        self,
        source: InterpretationClaimSource,
        ttl_seconds: int,
        owned: Execution,
    ) -> Claim | None:
        read_claim_plan(source, ttl_seconds, owned.plan, route_resolver=self._runtime.routing)
        write = InterpretationClaimWrite(
            self._store, source, ttl_seconds, owned.plan, route_resolver=self._runtime.routing
        )
        root = InterpretationStateRoot(
            self._store.transactions,
            self._archives,
            self._runtime.store,
            source.intent(ttl_seconds),
            owned.plan,
            write,
            database_seconds=self._database_seconds,
            route_resolver=self._runtime.routing,
        )
        try:
            outcome = await self._runtime.execute(owned, root)
        except TransactionSendError as error:
            outcome = error.result
            if outcome.local != LocalOutcome.COMMITTED and not (
                outcome.local == LocalOutcome.ABORTED and write.quota_denied
            ):
                raise
        if outcome.local == LocalOutcome.ABORTED and write.quota_denied:
            raise InterpretationClaimQuotaDenied("Original quota refused and claim root aborted")
        if outcome.local != LocalOutcome.COMMITTED:
            raise InterpretationClaimPending("Original claim root is not confirmed committed")
        await self._committed(source, ttl_seconds, owned)
        if write.staged is None:
            raise InterpretationClaimPending("Original claim staging result unavailable")
        return await self._handoff(write.staged.claim) if write.staged.claim is not None else None
