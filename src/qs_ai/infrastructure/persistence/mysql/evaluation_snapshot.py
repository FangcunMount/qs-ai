"""Establish an organization-scoped Run snapshot before reading its evidence."""

from sqlalchemy import select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def header(db: AsyncSession, scope: ManagementScope) -> RowMapping:
    # Do not depend on the production server's configurable default isolation level.
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    row = (
        (
            await db.execute(
                select(evaluation_runs, evaluation_checkpoints.c.version)
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
    return row
