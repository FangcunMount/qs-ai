"""Bounded keyset scanning; cursor is an optimization, never execution ownership."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.application.operations.diagnostics import emit
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import decode
from qs_ai.infrastructure.persistence.mysql.evaluation_recovery import recover_expired
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


@dataclass
class RecoveryCursor:
    after_run_id: str = ""
    after_candidate: tuple[str, str, int] = ("", "", 0)


async def recover_next(
    transactions: Transactions,
    routes: RouteAssets,
    schemas: SchemaAssets,
    actor: str,
    at: datetime,
    cursor: RecoveryCursor,
    *,
    batch_size: int = 64,
) -> bool:
    if type(batch_size) is not int or not 1 <= batch_size <= 256:
        raise ValueError("Invalid recovery scan bound")
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Recovery scan time requires a time zone")
    async with transactions.open() as db:
        rows = (
            (
                await db.execute(
                    select(
                        evaluation_runs.c.run_id,
                        evaluation_runs.c.organization_id,
                        evaluation_checkpoints.c.version,
                        evaluation_checkpoints.c.checkpoint_json,
                    )
                    .join(
                        evaluation_checkpoints,
                        evaluation_runs.c.run_id == evaluation_checkpoints.c.run_id,
                    )
                    .where(
                        evaluation_runs.c.run_id > cursor.after_run_id,
                        evaluation_runs.c.progress_json["status"].as_string() == "collecting",
                        func.json_type(evaluation_checkpoints.c.checkpoint_json) == "OBJECT",
                    )
                    .order_by(evaluation_runs.c.run_id)
                    .limit(batch_size)
                )
            )
            .mappings()
            .all()
        )
    if not rows:
        cursor.after_run_id = ""
        return False
    for row in rows:
        # Advance before trying: a damaged record must not permanently starve
        # later Runs. On restart/revisit every claim is revalidated transactionally.
        cursor.after_run_id = row["run_id"]
        cp = decode(row["checkpoint_json"])
        assert cp is not None
        if cp.lease_expires_at > at:
            continue
        try:
            async with transactions.open() as db:
                await recover_expired(
                    db,
                    UUID(row["run_id"]),
                    row["version"],
                    row["organization_id"],
                    cp.invocation_id,
                    cp.lease_expires_at,
                    at,
                    actor,
                    routes,
                    schemas,
                )
                await db.commit()
            emit(
                "evaluation.recovery_committed",
                "evaluation",
                run_id=str(row["run_id"]),
                invocation_id=cp.invocation_id,
            )
        except CheckpointConflict:
            continue
        return True
    if len(rows) < batch_size:
        cursor.after_run_id = ""
    return False
