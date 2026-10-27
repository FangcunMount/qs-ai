"""Recover exact candidate ownership using its first evidence, without provider capability."""

import asyncio
from datetime import datetime
from typing import Any

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import State

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_recovery import (
    read_candidate_recovery_source,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    CandidateResponseReference,
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
    CandidateCompletionSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_completion_state import (
    CandidateCompletionPlanner,
    read_candidate_completion_plan,
)
from qs_ai.infrastructure.workflow_transport.candidate_expiry_sender import CandidateExpirySender
from qs_ai.infrastructure.workflow_transport.candidate_expiry_state import (
    CandidateExpiryRequest,
    read_candidate_expiry_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class CandidateRecoveryRuntime:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        recover_aborted: bool = False,
    ) -> None:
        if type(recover_aborted) is not bool:
            raise ValueError("Explicit host aborted-recovery policy required")
        self.transactions, self.database_seconds = transactions, database_seconds
        self._runtime, self._recover_aborted = runtime, recover_aborted
        self._expiry = CandidateExpirySender(
            transactions,
            archives,
            runtime,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )
        self._completion_planner = CandidateCompletionPlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            route_resolver=runtime.routing,
        )
        self._completion = CandidateCompletionSender(
            transactions,
            archives,
            runtime,
            self._completion_planner,
        )

    async def __call__(self, organization_id: int, claim: SlotClaim, at: datetime) -> bool:
        CandidateExpiryRequest(organization_id, claim)
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            source = await read_candidate_recovery_source(
                db, organization_id, claim, at, lock=False
            )
            if source is None:
                return False
            request = CandidateExpiryRequest(organization_id, source.claim)
            found = (
                await read_candidate_response(db, organization_id, source.claim)
                if source.evidence is not None
                else None
            )
            if source.evidence is not None and (
                found is None or found.result.encode() != source.evidence.encode()
            ):
                raise CheckpointConflict("Exact first durable candidate response required")
        if found is not None:
            await self._finish_response(found.reference, source.version)
            return True
        return await self._finish_expiry(request, source.version, at)

    async def _finish_response(self, reference: CandidateResponseReference, version: int) -> None:
        if self._recover_aborted:
            intent = await self._completion_planner.intent(reference)
            async with asyncio.timeout(self.database_seconds):
                old = await self._runtime.store.lookup(intent.request)
            if old is not None and old.state == State.ABORTED:
                facts, first = read_candidate_completion_plan(
                    intent, old.execution.plan, route_resolver=self._runtime.routing
                )
                if (
                    first.reference != reference
                    or facts.source_version is None
                    or version < facts.source_version
                ):
                    raise MessageConflict("Original aborted candidate completion identity differs")
                if version == facts.source_version:
                    await self._completion.retry(reference, old.execution)
                else:
                    await self._completion.replan(reference, old.execution)
                return
        await self._completion(reference)

    async def _finish_expiry(
        self, request: CandidateExpiryRequest, version: int, at: datetime
    ) -> bool:
        if self._recover_aborted:
            async with asyncio.timeout(self.database_seconds):
                old = await self._runtime.store.lookup(request.intent.request)
            if old is not None and old.state == State.ABORTED:
                facts, first, _ = read_candidate_expiry_plan(
                    request, old.execution.plan, route_resolver=self._runtime.routing
                )
                if facts.source_version is None or version < facts.source_version:
                    raise MessageConflict("Candidate expiry coordinator regressed")
                if (
                    version == facts.source_version
                    and request.claim.checkpoint.lease_expires_at
                    == first.claim.checkpoint.lease_expires_at
                ):
                    return await self._expiry.retry(request, old.execution)
                return await self._expiry.replan(request, old.execution, at)
        return await self._expiry(request, at)
