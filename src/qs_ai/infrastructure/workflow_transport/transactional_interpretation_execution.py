"""Bounded original worker scan with explicit claim/finish transaction senders."""

import asyncio
import math
from typing import Any

from reliable_messaging.transaction import State

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim, EvidenceSet, WorkflowResult
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.interpretation_claim_deferral import (
    InterpretationClaimDeferral,
)
from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
    InterpretationClaimPending,
    InterpretationClaimQuotaDenied,
    InterpretationClaimSender,
)
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishSender,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import (
    AuthorizedStateTransactions,
    TransactionAdmissionClosed,
)


class TransactionalInterpretationExecutionStore:
    """Explicit opt-in host adapter; no startup, pool ownership or legacy fallback."""

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
            raise ValueError("Finite positive original worker database budget required")
        self._store, self._runtime = store, runtime
        self._database_seconds = database_seconds
        keys = dict(
            signing_key=signing_key, recipient_key=recipient_key, database_seconds=database_seconds
        )
        self._claims = InterpretationClaimSender(store, archives, runtime, **keys)
        self._finish = InterpretationFinishSender(store, archives, runtime, **keys)
        self._defer = InterpretationClaimDeferral(
            store, runtime.store, archives, database_seconds=database_seconds
        )

    def _admit(self) -> None:
        if not self._runtime.stats()["ready"]:
            raise TransactionAdmissionClosed("Original worker transaction admission closed")

    async def claim(self, ttl_seconds: int) -> Claim | None:
        if type(ttl_seconds) is not int or not 3 <= ttl_seconds <= 86400:
            raise ValueError("Bounded original worker TTL required")
        self._admit()
        async with asyncio.timeout(self._database_seconds):
            async with self._store.transactions.open() as db:
                candidates = await self._store.claim_candidates(db)
        # Unlike the legacy local scanner, no source connection/locks survive MQ I/O.
        for job_id, session_id in candidates:
            self._admit()
            try:
                source = await self._claims.source(job_id, session_id)
            except LeaseLost:
                continue
            async with asyncio.timeout(self._database_seconds):
                previous = await self._runtime.store.lookup(source.intent(ttl_seconds).request)
            if previous is not None and previous.state == State.PREPARED:
                continue  # Another exact execution is pending. Not new retry authority.
            try:
                if previous is not None and previous.state == State.ABORTED:
                    # Original persisted due job is the host decision. SDK still proves
                    # the old exact abort atomically; Checker never makes this call.
                    claim = await self._claims.retry(source, ttl_seconds, previous.execution)
                else:
                    claim = await self._claims(source, ttl_seconds)
            except InterpretationClaimQuotaDenied:
                async with asyncio.timeout(self._database_seconds):
                    aborted = await self._runtime.store.lookup(source.intent(ttl_seconds).request)
                if aborted is None or aborted.state != State.ABORTED:
                    raise InterpretationClaimPending(
                        "Original quota deferral abort unresolved"
                    ) from None
                await self._defer(source, ttl_seconds, aborted.execution)
                continue
            if claim is not None or source.exhausted:
                return claim
        return None

    async def renew(self, claim: Claim, ttl_seconds: int) -> None:
        await self._store.renew(claim, ttl_seconds)

    async def evidence(self, claim: Claim) -> EvidenceSet | None:
        return await self._store.evidence(claim)

    async def finish(self, claim: Claim, result: WorkflowResult) -> None:
        await self._finish(claim, result)
