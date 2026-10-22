"""Explicit candidate host assembly; construction starts no clients or background tasks."""

import asyncio
from uuid import UUID

from reliable_messaging.durable import MessageConflict

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    read_candidate_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_step import PreparedStep
from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
    CandidateCompletionSender,
)
from qs_ai.infrastructure.workflow_transport.candidate_dispatch_sender import (
    CandidateDispatchSender,
)


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
