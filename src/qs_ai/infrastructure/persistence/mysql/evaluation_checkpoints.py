from dataclasses import asdict
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints as table


def encode(checkpoint: ExecutionCheckpoint | None) -> dict | None:
    if checkpoint is None:
        return None
    value = asdict(checkpoint)
    for key in ("claimed_at", "lease_expires_at", "dispatch_started_at"):
        if value[key] is not None:
            value[key] = value[key].isoformat()
    return value


def decode(value: dict | None) -> ExecutionCheckpoint | None:
    if value is None:
        return None
    fields = dict(value)
    for key in ("claimed_at", "lease_expires_at", "dispatch_started_at"):
        if fields[key] is not None:
            fields[key] = datetime.fromisoformat(fields[key])
    return ExecutionCheckpoint(**fields)


class MySQLCheckpoints:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def create(self, run_id: UUID) -> CheckpointState:
        try:
            async with self.transactions.open() as db:
                await db.execute(insert(table).values(run_id=str(run_id), version=0))
                await db.commit()
        except IntegrityError as error:
            if error.orig is not None and error.orig.args and error.orig.args[0] == 1062:
                raise CheckpointConflict("Checkpoint state already exists") from None
            raise
        return CheckpointState(run_id, 0, None)

    async def get(self, run_id: UUID) -> CheckpointState | None:
        async with self.transactions.open() as db:
            row = (
                (await db.execute(select(table).where(table.c.run_id == str(run_id))))
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return CheckpointState(run_id, row["version"], decode(row["checkpoint_json"]))

    async def save(self, state: CheckpointState, expected_version: int) -> None:
        async with self.transactions.open() as db:
            await save_checkpoint(db, state, expected_version)
            await db.commit()


async def save_checkpoint(db: AsyncSession, state: CheckpointState, expected_version: int) -> None:
    """Participate in the caller's transaction; never commit or roll back other aggregate writes."""
    if (
        type(expected_version) is not int
        or expected_version < 0
        or state.version != expected_version + 1
    ):
        raise ValueError("Checkpoint updates must advance exactly one version")
    result = await db.execute(
        update(table)
        .where(table.c.run_id == str(state.run_id), table.c.version == expected_version)
        .values(version=state.version, checkpoint_json=encode(state.checkpoint))
    )
    if cast(CursorResult, result).rowcount != 1:
        raise CheckpointConflict("Checkpoint state changed concurrently")
