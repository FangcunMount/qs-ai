"""Admission uses the same transaction as Start or an authorized resumption."""

import asyncio
from datetime import UTC, datetime

from sqlalchemy import func, insert, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.capacity import (
    CapacityExceeded,
    EvaluationCapacityPolicy,
    EvaluationCapacityReservation,
    EvaluationCapacitySnapshot,
)
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.quotas import evaluation_policy
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_admission_locks as locks,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_capacity_reservations as reservations,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_run_policies,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


async def lock_admission(db: AsyncSession, organization_id: int) -> None:
    # Must precede the first snapshot read and every checkpoint/Run lock.
    stmt = mysql_insert(locks).values(organization_id=organization_id)
    await db.execute(stmt.on_duplicate_key_update(organization_id=organization_id))


async def admit(
    db: AsyncSession,
    scope: ManagementScope,
    policy: EvaluationCapacityPolicy,
    at: datetime,
    quota_baseline: QuotaBaseline | None = None,
) -> None:
    """Caller holds organization admission lock; failures roll back the Run transition."""
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Admission time must have a time zone")
    policy, quota_snapshot = await evaluation_policy(
        db, scope.organization_id, policy, quota_baseline
    )
    active = (
        await db.execute(
            select(func.count())
            .select_from(evaluation_runs)
            .where(
                evaluation_runs.c.organization_id == scope.organization_id,
                evaluation_runs.c.run_id != str(scope.run_id),
                evaluation_runs.c.progress_json["status"].as_string() == "collecting",
            )
        )
    ).scalar_one()
    if active >= policy.max_active_runs:
        raise CapacityExceeded("Organization active evaluation limit reached")
    prior = (
        (await db.execute(select(reservations).where(reservations.c.run_id == str(scope.run_id))))
        .mappings()
        .one_or_none()
    )
    if prior is not None:
        if prior["organization_id"] != scope.organization_id:
            raise ValueError("Evaluation reservation organization mismatch")
        # Resuming the same frozen Run consumes its original worst-case reservation.
        return
    frozen = await asyncio.to_thread(load_execution_policy)
    stored = (
        (
            await db.execute(
                select(evaluation_run_policies).where(
                    evaluation_run_policies.c.run_id == str(scope.run_id)
                )
            )
        )
        .mappings()
        .one()
    )
    if (stored["definition_json"], stored["fingerprint"]) != (
        frozen.definition_json,
        frozen.fingerprint,
    ):
        raise ValueError("Unrecognized frozen evaluation budget")
    calls = frozen.generation_per_run + frozen.semantic_per_run
    day = at.astimezone(UTC).date()
    used = (
        await db.execute(
            select(func.coalesce(func.sum(reservations.c.provider_calls), 0)).where(
                reservations.c.organization_id == scope.organization_id,
                reservations.c.budget_day == day,
            )
        )
    ).scalar_one()
    if used + calls > policy.daily_provider_calls:
        raise CapacityExceeded("Organization daily evaluation budget reached")
    await db.execute(
        insert(reservations).values(
            run_id=str(scope.run_id),
            organization_id=scope.organization_id,
            budget_day=day,
            provider_calls=calls,
            daily_limit=policy.daily_provider_calls,
            quota_snapshot=quota_snapshot,
            requested_by=scope.actor,
            reserved_at=at.astimezone(UTC).replace(tzinfo=None),
        )
    )


class MySQLEvaluationCapacity:
    def __init__(
        self,
        transactions: Transactions,
        capacity: EvaluationCapacityPolicy,
        quota_baseline: QuotaBaseline | None = None,
    ) -> None:
        self.transactions, self.capacity = transactions, capacity
        self.quota_baseline = quota_baseline

    async def get(self, scope: DraftScope, at: datetime) -> EvaluationCapacitySnapshot:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("Capacity query time must have a time zone")
        day = at.astimezone(UTC).date()
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            capacity, _ = await evaluation_policy(
                db, scope.organization_id, self.capacity, self.quota_baseline
            )
            matching = (
                reservations.c.organization_id == scope.organization_id,
                reservations.c.budget_day == day,
            )
            reserved, count = (
                await db.execute(
                    select(
                        func.coalesce(func.sum(reservations.c.provider_calls), 0), func.count()
                    ).where(*matching)
                )
            ).one()
            active = (
                await db.execute(
                    select(func.count())
                    .select_from(evaluation_runs)
                    .where(
                        evaluation_runs.c.organization_id == scope.organization_id,
                        evaluation_runs.c.progress_json["status"].as_string() == "collecting",
                    )
                )
            ).scalar_one()
            receipts = (
                (
                    await db.execute(
                        select(reservations)
                        .where(*matching)
                        .order_by(reservations.c.reserved_at.desc(), reservations.c.run_id.desc())
                        .limit(100)
                    )
                )
                .mappings()
                .all()
            )
        reserved = int(reserved)
        frozen = await asyncio.to_thread(load_execution_policy)
        full = frozen.generation_per_run + frozen.semantic_per_run
        remaining = max(0, capacity.daily_provider_calls - reserved)
        return EvaluationCapacitySnapshot(
            scope.organization_id,
            day.isoformat(),
            capacity.daily_provider_calls,
            reserved,
            remaining,
            full,
            remaining // full,
            capacity.max_active_runs,
            active,
            count,
            tuple(
                EvaluationCapacityReservation(
                    r["run_id"],
                    r["provider_calls"],
                    r["requested_by"],
                    r["reserved_at"].replace(tzinfo=UTC).isoformat(),
                )
                for r in receipts
            ),
            count > len(receipts),
        )
