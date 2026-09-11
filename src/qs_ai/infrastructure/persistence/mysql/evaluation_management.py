import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_progress import transition_requested
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import accept_resolution
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def read_view(db: AsyncSession, scope: ManagementScope) -> EvaluationView:
    row = (
        (
            await db.execute(
                select(evaluation_runs.c.progress_json, evaluation_checkpoints.c.version)
                .join(
                    evaluation_checkpoints,
                    evaluation_runs.c.run_id == evaluation_checkpoints.c.run_id,
                )
                .where(
                    evaluation_runs.c.run_id == str(scope.run_id),
                    evaluation_runs.c.organization_id == scope.organization_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFound("Evaluation unavailable in organization")
    progress = row["progress_json"]
    if progress is None:
        raise ValueError("Legacy progress needs reconciliation")
    return EvaluationView(
        str(scope.run_id),
        row["version"],
        progress["status"],
        progress.get("unresolved_result_unknown_count", 0),
        json.dumps(progress.get("result_unknown_resolutions", []), ensure_ascii=False),
    )


class MySQLEvaluationManagement:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def get(self, scope: ManagementScope) -> EvaluationView:
        async with self.transactions.open() as db:
            return await read_view(db, scope)

    async def start(
        self,
        scope: ManagementScope,
        expected_version: int,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        if confirm is not True or type(expected_version) is not int or expected_version < 1:
            raise ValueError("Explicit version and confirmation required")
        async with self.transactions.open() as db:
            # Scope lookup precedes state changes; absent and foreign Runs look identical.
            await read_view(db, scope)
            await transition_requested(
                db,
                scope.run_id,
                expected_version,
                scope.organization_id,
                "collecting",
                scope.actor,
                reason,
                at,
            )
            view = await read_view(db, scope)
            await db.commit()
            return view

    async def resolve(
        self,
        scope: ManagementScope,
        expected_version: int,
        value: ResultUnknownResolution,
        *,
        confirm: bool,
    ) -> EvaluationView:
        if value.actor != scope.actor:
            raise ValueError("Decision actor differs from trusted scope")
        async with self.transactions.open() as db:
            await accept_resolution(
                db, scope.run_id, expected_version, scope.organization_id, value, confirm=confirm
            )
            view = await read_view(db, scope)
            await db.commit()
            return view
