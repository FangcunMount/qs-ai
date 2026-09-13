"""Read current limits and persisted reservations in one repeatable snapshot."""

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import ColumnElement, case, func, select

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.management import (
    ParticipantCapacityQuery,
    ParticipantCapacitySnapshot,
    ParticipantCapacityUsage,
    ParticipantReservation,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import participant_capacity_reservations as rows


class MySQLParticipantCapacityReader:
    def __init__(self, transactions: Transactions, capacity: ParticipantCapacityPolicy) -> None:
        self.transactions, self.policy = transactions, capacity

    async def get(
        self, query: ParticipantCapacityQuery, at: datetime
    ) -> ParticipantCapacitySnapshot:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("Capacity query time must have a timezone")
        day = at.astimezone(UTC).date()
        org = rows.c.organization_id == query.scope.organization_id
        today = rows.c.budget_day == day
        active = rows.c.active.is_(True)
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            async def usage(
                identity: str, predicate: ColumnElement[Any], daily_limit: int, active_limit: int
            ) -> ParticipantCapacityUsage:
                daily, running = (
                    await db.execute(
                        select(
                            func.coalesce(func.sum(case((today, 1), else_=0)), 0),
                            func.coalesce(func.sum(case((active, 1), else_=0)), 0),
                        ).where(org, predicate, today | active)
                    )
                ).one()
                daily, running = int(daily), int(running)
                return ParticipantCapacityUsage(
                    identity,
                    daily,
                    max(0, daily_limit - daily),
                    running,
                    max(0, active_limit - running),
                )

            organization = await usage(
                str(query.scope.organization_id), org, self.policy.daily_org, self.policy.active_org
            )
            subject = (
                await usage(
                    query.subject_id,
                    rows.c.subject_id == query.subject_id,
                    self.policy.daily_user,
                    self.policy.active_user,
                )
                if query.subject_id
                else None
            )
            assessment = (
                await usage(
                    query.assessment_id,
                    func.json_contains(rows.c.assessment_ids, json.dumps(query.assessment_id)) == 1,
                    self.policy.daily_assessment,
                    self.policy.active_assessment,
                )
                if query.assessment_id
                else None
            )

            async def receipts(predicate: ColumnElement[Any]) -> tuple[ParticipantReservation, ...]:
                data = await db.execute(
                    select(rows)
                    .where(org, predicate)
                    .order_by(rows.c.reserved_at.desc(), rows.c.run_id.desc())
                    .limit(100)
                )
                return tuple(
                    ParticipantReservation(
                        r["run_id"],
                        r["session_id"],
                        r["subject_id"],
                        tuple(r["assessment_ids"]),
                        r["budget_day"].isoformat(),
                        r["reserved_at"].replace(tzinfo=UTC).isoformat(),
                        r["active"],
                        r["acquired_at"].replace(tzinfo=UTC).isoformat()
                        if r["acquired_at"]
                        else "",
                    )
                    for r in data.mappings()
                )

            daily_rows, active_rows = await receipts(today), await receipts(active)
        return ParticipantCapacitySnapshot(
            query.scope.organization_id,
            day.isoformat(),
            self.policy,
            organization,
            subject,
            assessment,
            daily_rows,
            active_rows,
            organization.daily_reserved > len(daily_rows),
            organization.active > len(active_rows),
        )
