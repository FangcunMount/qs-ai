"""Pair original serial model dispatch with first evidence and transactional completion."""

import asyncio
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from reliable_messaging.durable import MessageConflict
from reliable_messaging.sqlalchemy import bind
from reliable_messaging.transaction import BindingError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    SerialResponseReference,
    read_serial_response,
    save_serial_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import PreparedStep
from qs_ai.infrastructure.workflow_transport.serial_generation_sender import SerialGenerationSender
from qs_ai.infrastructure.workflow_transport.serial_semantic_sender import SerialSemanticSender
from qs_ai.infrastructure.workflow_transport.worker_dispatch_sender import (
    WorkerDispatchRecovery,
    WorkerDispatchSender,
)


class _SerialResponseEvents:
    """Response persistence is an operational root, never an event-producing business change."""

    def __init__(self, db: AsyncSession) -> None:
        root = db.get_transaction()
        if (
            root is None
            or not root.is_active
            or db.in_nested_transaction()
            or db.info.get("evaluation_events")
        ):
            raise BindingError("Clean original serial response root required")
        self._db, self._root = db, root

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        raise BindingError("Serial response root cannot stage interpretation state")

    async def flush(self, db: AsyncSession) -> None:
        if (
            db is not self._db
            or db.get_transaction() is not self._root
            or not self._root.is_active
            or db.in_nested_transaction()
            or db.info.get("state_events") is not self
            or db.info.get("evaluation_events")
        ):
            raise BindingError("Serial response root or event boundary changed")


class SerialRuntime:
    def __init__(
        self,
        dispatch: WorkerDispatchSender,
        generation: SerialGenerationSender,
        semantic: SerialSemanticSender,
        *,
        dispatch_recovery: WorkerDispatchRecovery | None = None,
    ) -> None:
        if (
            dispatch._transactions is not generation._transactions
            or dispatch._transactions is not semantic._transactions
            or dispatch._runtime is not generation._runtime
            or dispatch._runtime is not semantic._runtime
            or dispatch._archives is not generation._archives
            or dispatch._archives is not semantic._archives
        ):
            raise ValueError("Serial preparation and completion must share the original host")
        if dispatch_recovery is not None and dispatch_recovery.sender is not dispatch:
            raise ValueError("Serial dispatch recovery must own this exact sender")
        self.transactions = dispatch._transactions
        self.dispatch, self.generation, self.semantic = dispatch, generation, semantic
        self._dispatch_recovery = dispatch_recovery

    async def prepare(self, run_id: UUID, version: int, organization_id: int) -> PreparedStep:
        if self._dispatch_recovery is not None:
            return await self._dispatch_recovery(run_id, version, organization_id)
        return await self.dispatch(run_id, version, organization_id)

    async def save_response(
        self, organization_id: int, prepared: PreparedStep, result: EvaluationResponse
    ) -> None:
        reference = SerialResponseReference.from_prepared(organization_id, prepared)
        async with (
            asyncio.timeout(self.generation._planner.database_seconds),
            self.transactions.open() as db,
        ):
            await db.begin()
            events = _SerialResponseEvents(db)
            db.info["state_events"] = events
            original = bind(db)
            await original.validate()
            await save_serial_response(db, reference, result)
            await original.validate()
            await events.flush(db)
            await db.commit()

    async def complete(
        self, organization_id: int, prepared: PreparedStep, result: EvaluationResponse
    ) -> CheckpointState:
        reference = SerialResponseReference.from_prepared(organization_id, prepared)
        async with (
            asyncio.timeout(self.generation._planner.database_seconds),
            self.transactions.open() as db,
        ):
            first = await read_serial_response(db, reference)
            if first is None or first.encode() != result.encode():
                raise MessageConflict("First durable serial response differs from host result")
        if reference.checkpoint.kind == "generation":
            return await self.generation(reference)
        return await self.semantic(reference)
