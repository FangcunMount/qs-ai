"""Budget at admission; active slots survive lease recovery and release at terminal commit."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.domain.interpretation.model import RuleViolation, Session
from qs_ai.infrastructure.persistence.mysql.quotas import participant_policy
from qs_ai.infrastructure.persistence.mysql.schema import (
    participant_admission_locks as locks,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    participant_capacity_reservations as reservations,
)


async def lock(db: AsyncSession, session: Session) -> None:
    # Commands/workers consistently lock Session -> Job/Lease -> organization.
    org = int(session.actor.org_id)
    stmt = mysql_insert(locks).values(organization_id=org)
    await db.execute(stmt.on_duplicate_key_update(organization_id=org))


def fits(rows: Sequence[Mapping[Any, Any]], session: Session, limits: tuple[int, int, int]) -> bool:
    org_limit, user_limit, assessment_limit = limits
    return (
        len(rows) < org_limit
        and sum(r["subject_id"] == session.actor.subject_id for r in rows) < user_limit
        and all(
            sum(a in r["assessment_ids"] for r in rows) < assessment_limit
            for a in session.assessment_ids
        )
    )


async def prior(db: AsyncSession, session: Session) -> Mapping[Any, Any] | None:
    row = (
        (
            await db.execute(
                select(reservations)
                .where(reservations.c.run_id == session.active_run_id)
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is not None and (
        row["session_id"] != session.id
        or row["organization_id"] != int(session.actor.org_id)
        or row["subject_id"] != session.actor.subject_id
        or row["assessment_ids"] != list(session.assessment_ids)
    ):
        raise RuleViolation("participant_reservation_conflict")
    return row


async def reserve(
    db: AsyncSession,
    session: Session,
    policy: ParticipantCapacityPolicy,
    at: datetime,
    quota_baseline: QuotaBaseline | None = None,
) -> None:
    if at.tzinfo is None or not session.active_run_id:
        raise ValueError("Participant reservation requires UTC time and Run")
    await lock(db, session)
    if await prior(db, session) is not None:
        return
    policy, quota_snapshot = await participant_policy(
        db, int(session.actor.org_id), policy, quota_baseline
    )
    day = at.astimezone(UTC).date()
    # Current locking reads avoid an older snapshot established by idempotency lookup.
    rows = (
        (
            await db.execute(
                select(reservations)
                .where(
                    reservations.c.organization_id == int(session.actor.org_id),
                    reservations.c.budget_day == day,
                )
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    if not fits(rows, session, (policy.daily_org, policy.daily_user, policy.daily_assessment)):
        raise RuleViolation("participant_daily_capacity_exceeded")
    await db.execute(
        insert(reservations).values(
            run_id=session.active_run_id,
            session_id=session.id,
            organization_id=int(session.actor.org_id),
            subject_id=session.actor.subject_id,
            assessment_ids=list(session.assessment_ids),
            budget_day=day,
            reserved_at=at.astimezone(UTC).replace(tzinfo=None),
            active=False,
            quota_snapshot=quota_snapshot,
        )
    )


async def acquire(
    db: AsyncSession,
    session: Session,
    policy: ParticipantCapacityPolicy,
    at: datetime,
    quota_baseline: QuotaBaseline | None = None,
) -> bool:
    await lock(db, session)
    row = await prior(db, session)
    if row is None:
        # Pre-migration queued work cannot silently bypass the budget on its first call.
        try:
            await reserve(db, session, policy, at, quota_baseline)
        except RuleViolation as error:
            if error.code != "participant_daily_capacity_exceeded":
                raise
            return False
    elif row["active"]:
        return True
    elif row["released_at"] is not None:
        raise RuleViolation("participant_slot_already_released")
    rows = (
        (
            await db.execute(
                select(reservations)
                .where(
                    reservations.c.organization_id == int(session.actor.org_id),
                    reservations.c.active.is_(True),
                )
                .with_for_update()
            )
        )
        .mappings()
        .all()
    )
    policy, _ = await participant_policy(db, int(session.actor.org_id), policy, quota_baseline)
    if not fits(rows, session, (policy.active_org, policy.active_user, policy.active_assessment)):
        return False
    await db.execute(
        update(reservations)
        .where(reservations.c.run_id == session.active_run_id)
        .values(active=True, acquired_at=at.astimezone(UTC).replace(tzinfo=None))
    )
    return True


async def release(db: AsyncSession, session: Session, at: datetime) -> None:
    await lock(db, session)
    await db.execute(
        update(reservations)
        .where(
            reservations.c.run_id == session.active_run_id,
            reservations.c.organization_id == int(session.actor.org_id),
            reservations.c.active.is_(True),
        )
        .values(active=False, released_at=at.astimezone(UTC).replace(tzinfo=None))
    )
