"""Bounded scan dispatch selects original durable response or interrupted expiry, never a model."""

import asyncio
from datetime import datetime
from typing import Any
from uuid import UUID

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import State

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_recovery import read_expired_recovery_source
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    SerialResponseReference,
    read_serial_response_reference,
)
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.expired_recovery_sender import ExpiredRecoverySender
from qs_ai.infrastructure.workflow_transport.expired_recovery_state import (
    ExpiredRecoveryRequest,
    read_expired_recovery_plan,
)
from qs_ai.infrastructure.workflow_transport.serial_generation_sender import SerialGenerationSender
from qs_ai.infrastructure.workflow_transport.serial_generation_state import (
    SerialGenerationPlanner,
    read_serial_generation_plan,
)
from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import SerialSemanticSender
from qs_ai.infrastructure.workflow_transport.serial_semantic_state import (
    SerialSemanticPlanner,
    read_serial_semantic_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_root import AuthorizedStateTransactions


class SerialRecoveryRuntime:
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        runtime: AuthorizedStateTransactions,
        routes: RouteAssets,
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
        self._expired = ExpiredRecoverySender(
            transactions,
            archives,
            runtime,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
        )
        self._generation_planner = SerialGenerationPlanner(
            transactions,
            routes,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            route_resolver=runtime.routing,
        )
        self._generation = SerialGenerationSender(
            transactions,
            archives,
            runtime,
            self._generation_planner,
        )
        self._semantic_planner = SerialSemanticPlanner(
            transactions,
            signing_key=signing_key,
            recipient_key=recipient_key,
            database_seconds=database_seconds,
            route_resolver=runtime.routing,
        )
        self._semantic = SerialSemanticSender(
            transactions,
            archives,
            runtime,
            self._semantic_planner,
        )

    async def __call__(
        self,
        run_id: UUID,
        version: int,
        organization_id: int,
        invocation_id: str,
        observed_expiry: datetime,
        at: datetime,
        actor: str,
    ) -> bool:
        request = ExpiredRecoveryRequest(
            run_id, organization_id, version, invocation_id, observed_expiry
        )
        async with asyncio.timeout(self.database_seconds), self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            source = await read_expired_recovery_source(
                db,
                run_id,
                version,
                organization_id,
                invocation_id,
                observed_expiry,
                at,
                actor,
                lock=False,
            )
            if source.run["execution_mode"] != "serial_v1":
                raise CheckpointConflict("Original serial recovery ownership required")
            reference = (
                await read_serial_response_reference(db, run_id, organization_id, invocation_id)
                if source.checkpoint.phase == "dispatching"
                else None
            )
            if reference is not None and reference.checkpoint != source.checkpoint:
                raise CheckpointConflict(
                    "Original durable serial dispatch differs from expiry hint"
                )
        # Close the observation boundary before preparing halves or opening the
        # separate guarded business root. Neither path has a provider gateway.
        if reference is None:
            return await self._finish_expiry(request, actor, at)
        await self._finish_response(reference, version)
        return True

    async def _finish_expiry(
        self, request: ExpiredRecoveryRequest, actor: str, at: datetime
    ) -> bool:
        if self._recover_aborted:
            async with asyncio.timeout(self.database_seconds):
                old = await self._runtime.store.lookup(request.intent.request)
            if old is not None and old.state == State.ABORTED:
                read_expired_recovery_plan(
                    request, old.execution.plan, route_resolver=self._runtime.routing
                )
                return await self._expired.retry(request, old.execution)
        return await self._expired(request, actor, at)

    async def _finish_response(self, reference: SerialResponseReference, version: int) -> None:
        if not self._recover_aborted:
            if reference.checkpoint.kind == "generation":
                await self._generation(reference)
            else:
                await self._semantic(reference)
            return
        if reference.checkpoint.kind == "generation":
            intent = await self._generation_planner.intent(reference)
        else:
            intent = await self._semantic_planner.intent(reference)
        async with asyncio.timeout(self.database_seconds):
            old = await self._runtime.store.lookup(intent.request)
        if old is not None and old.state == State.ABORTED:
            if reference.checkpoint.kind == "generation":
                facts, generation = read_serial_generation_plan(
                    intent, old.execution.plan, route_resolver=self._runtime.routing
                )
                first_reference = generation.reference
            else:
                facts, semantic = read_serial_semantic_plan(
                    intent, old.execution.plan, route_resolver=self._runtime.routing
                )
                first_reference = semantic.reference
            if (
                first_reference != reference
                or facts.source_version is None
                or version < facts.source_version
            ):
                raise MessageConflict("Original aborted serial completion identity differs")
            if reference.checkpoint.kind == "generation":
                if version == facts.source_version:
                    await self._generation.retry(reference, old.execution)
                else:
                    await self._generation.replan(reference, old.execution)
            elif version == facts.source_version:
                await self._semantic.retry(reference, old.execution)
            else:
                await self._semantic.replan(reference, old.execution)
        elif reference.checkpoint.kind == "generation":
            await self._generation(reference)
        else:
            await self._semantic(reference)
