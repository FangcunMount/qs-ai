"""Take over an expired response's lease without granting a new business attempt."""

from uuid import UUID

from sqlalchemy import func, select, update

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim
from qs_ai.domain.interpretation.model import Status
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore, expiry
from qs_ai.infrastructure.persistence.mysql.interpretation import session_from
from qs_ai.infrastructure.persistence.mysql.participant_capacity import prior
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_leases,
    jobs,
    model_calls,
    sessions,
)


async def claim_finalization(
    store: MySQLExecutionStore, job_id: str, session_id: str, ttl_seconds: int
) -> Claim | None:
    if type(ttl_seconds) is not int or not 3 <= ttl_seconds <= 86400:
        raise ValueError("Bounded finalization lease TTL required")
    try:
        if any(not isinstance(v, str) or str(UUID(v)) != v for v in (job_id, session_id)):
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Canonical finalization candidate required") from None
    async with store.transactions.open() as db:
        # Advisory probe only. The locked reads below recheck every authority.
        response = await db.scalar(
            select(model_calls.c.run_id)
            .join(jobs, jobs.c.run_id == model_calls.c.run_id)
            .where(
                jobs.c.id == job_id,
                jobs.c.session_id == session_id,
                model_calls.c.status == "response_received",
            )
        )
        if response is None:
            return None
        row = (
            (
                await db.execute(
                    select(sessions)
                    .where(sessions.c.id == session_id)
                    .with_for_update(skip_locked=True)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        session = session_from(row)
        if session.status != Status.RUNNING:
            return None
        job = (
            (
                await db.execute(
                    select(jobs)
                    .where(
                        jobs.c.id == job_id,
                        jobs.c.session_id == session.id,
                        jobs.c.run_id == session.active_run_id,
                        jobs.c.status == "leased",
                        jobs.c.lease_until <= func.utc_timestamp(6),
                    )
                    .with_for_update(skip_locked=True)
                )
            )
            .mappings()
            .one_or_none()
        )
        if job is None:
            return None
        lease = (
            (
                await db.execute(
                    select(
                        execution_leases,
                        (execution_leases.c.expires_at <= func.utc_timestamp(6)).label("expired"),
                    )
                    .where(execution_leases.c.thread_id == session.id)
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if lease is None or not lease["expired"]:
            return None
        call = (
            (
                await db.execute(
                    select(model_calls)
                    .where(
                        model_calls.c.run_id == job["run_id"],
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if call is None or call["status"] != "response_received":
            return None
        if (
            not call["response_json"]
            or lease["fence"] != job["fence_token"]
            or not 0 < call["fence_token"] <= lease["fence"]
            or not 0 < lease["fence"] < 2**63 - 1
        ):
            raise LeaseLost("Finalization response or lease evidence conflicts")
        if session.uses_qs_snapshot:
            reservation = await prior(db, session)
            if reservation is None or not reservation["active"] or reservation["released_at"]:
                raise LeaseLost("Finalization requires its existing active participant slot")
        fence = lease["fence"] + 1
        await db.execute(
            update(execution_leases)
            .where(
                execution_leases.c.thread_id == session.id,
            )
            .values(fence=fence, expires_at=expiry(ttl_seconds))
        )
        await db.execute(
            update(jobs)
            .where(jobs.c.id == job_id)
            .values(
                fence_token=fence,
                lease_until=expiry(ttl_seconds),
            )
        )
        # No Session.running(), event, business attempt or participant/model admission.
        # An uncertain commit yields no Claim; the committed lease still fences peers.
        await db.commit()
        return Claim(
            job_id, job["run_id"], session, fence, job["answer"], job["skipped"], job["question_id"]
        )
