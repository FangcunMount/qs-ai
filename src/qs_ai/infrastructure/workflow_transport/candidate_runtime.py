"""Explicit candidate host assembly; construction starts no clients or background tasks."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from reliable_messaging.durable import MessageConflict
from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import BindingError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    EvaluationResponse,
    save_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    lock_run,
    renew_claim,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import PreparedStep
from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
    CandidateCompletionSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
    CandidateDispatchSender,
)


class _CandidateLocalEvents:
    """One response/lease root cannot publish or stage a visible business transition."""

    def __init__(self, db: AsyncSession) -> None:
        root = db.get_transaction()
        if (
            root is None
            or not root.is_active
            or db.in_nested_transaction()
            or db.info.get("evaluation_events")
        ):
            raise BindingError("Clean original candidate operational root required")
        self._db, self._root = db, root

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        raise BindingError("Candidate operational root cannot stage interpretation state")

    async def flush(self, db: AsyncSession) -> None:
        if (
            db is not self._db
            or db.get_transaction() is not self._root
            or not self._root.is_active
            or db.in_nested_transaction()
            or db.info.get("state_events") is not self
            or db.info.get("evaluation_events")
        ):
            raise BindingError("Candidate operational root or event boundary changed")


class CandidateRuntime:
    def __init__(
        self, dispatch: CandidateDispatchSender, completion: CandidateCompletionSender
    ) -> None:
        if dispatch.transactions is not completion.transactions:
            raise ValueError("Candidate preparation and completion must share the original host")
        self.dispatch, self.completion = dispatch, completion
        self.transactions = dispatch.transactions
        self.limit = dispatch.planner.limit

    async def prepare(self, run_id: UUID, version: int, organization_id: int) -> PreparedStep:
        return await self.dispatch(run_id, version, organization_id)

    async def _commit_local(self, write: Callable[[AsyncSession], Awaitable[None]]) -> None:
        async with (
            asyncio.timeout(self.completion.database_seconds),
            self.transactions.open() as db,
        ):
            await db.begin()
            events = _CandidateLocalEvents(db)
            db.info["state_events"] = events
            original = bind(db)
            await original.validate()
            await write(db)
            await original.validate()
            await events.flush(db)
            # EventSession still performs the normal original root commit. Its
            # local guard validates again; the host's global recorder is untouched.
            await db.commit()

    async def save_response(
        self, organization_id: int, prepared: PreparedStep, result: EvaluationResponse, at: datetime
    ) -> None:
        if prepared.claim is None:
            raise MessageConflict("Original candidate response claim required")
        claim = prepared.claim

        async def write(db: AsyncSession) -> None:
            await save_response(db, organization_id, claim, result, at=at)

        await self._commit_local(write)

    async def renew(
        self, organization_id: int, claim: SlotClaim, at: datetime, expires_at: datetime
    ) -> None:
        async def write(db: AsyncSession) -> None:
            await lock_run(db, claim.run_id, organization_id)
            await renew_claim(db, claim, at, expires_at)

        await self._commit_local(write)

    async def complete(
        self, organization_id: int, prepared: PreparedStep, result: EvaluationResponse
    ) -> CheckpointState:
        if prepared.claim is None:
            raise MessageConflict("Candidate completion requires its original dispatched claim")
        async with (
            asyncio.timeout(self.completion.database_seconds),
            self.transactions.open() as db,
        ):
            first = await read_candidate_response(db, organization_id, prepared.claim)
            if first is None or first.result != result:
                raise MessageConflict("First durable candidate response differs from host result")
        return await self.completion(first.reference)
