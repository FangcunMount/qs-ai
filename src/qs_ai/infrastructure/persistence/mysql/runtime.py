"""Bounded projections only: never select report, Prompt or model response bodies."""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import case, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.retry import retry_available
from qs_ai.application.execution.runtime import session_ids
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_configurations as configurations,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    external_requests,
    jobs,
    model_calls,
    result_outbox,
    runs,
    runtime_milestones,
    sessions,
)


def value(row: Mapping[Any, Any]) -> dict[str, Any]:
    # Only explicitly UTC fields reach this serializer. Legacy wall clocks are
    # exposed separately as zone-less strings, never interpreted using today's DB zone.
    return {
        key: item.replace(tzinfo=UTC).isoformat() if isinstance(item, datetime) else item
        for key, item in row.items()
    }


class MySQLRuntimeReader:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def _snapshot(self, db: AsyncSession) -> str:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        await db.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        return (await db.scalar(text("SELECT UTC_TIMESTAMP(6)"))).replace(tzinfo=UTC).isoformat()

    async def _summaries(
        self, db: AsyncSession, scope: DraftScope, ids: tuple[str, ...]
    ) -> list[dict[str, Any]]:
        query = (
            select(
                sessions.c.id.label("session_id"),
                external_requests.c.request_id,
                sessions.c.active_run_id.label("run_id"),
                sessions.c.status,
                sessions.c.version,
                sessions.c.workflow_version,
                sessions.c.failure_code,
                sessions.c.created_at_utc.label("created_at"),
                # An older writer after a code rollback only updates updated_at.
                # Do not present the previous UTC marker as its new update time.
                case(
                    (sessions.c.updated_at == sessions.c.updated_at_utc, sessions.c.updated_at_utc),
                    else_=None,
                ).label("updated_at"),
                model_calls.c.status.label("model_call_status"),
                model_calls.c.invocation_id,
                configurations.c.publication_id,
                configurations.c.publication_sha256,
            )
            .select_from(
                sessions.join(external_requests, external_requests.c.session_id == sessions.c.id)
                .outerjoin(model_calls, model_calls.c.run_id == sessions.c.active_run_id)
                .outerjoin(configurations, configurations.c.session_id == sessions.c.id)
            )
            .where(sessions.c.org_id == scope.organization_id, sessions.c.id.in_(ids))
            .order_by(sessions.c.id)
        )
        return [value(row) for row in (await db.execute(query)).mappings()]

    async def summaries(self, scope: DraftScope, ids: tuple[str, ...]) -> dict[str, Any]:
        session_ids(list(ids))
        async with asyncio.timeout(3), self.transactions.open() as db:
            observed_at = await self._snapshot(db)
            items = await self._summaries(db, scope, ids)
        found = {item["session_id"] for item in items}
        return {
            "observed_at": observed_at,
            "items": items,
            # Missing and foreign-organization records are intentionally indistinguishable.
            "unavailable_session_ids": [sid for sid in ids if sid not in found],
        }

    async def detail(self, scope: DraftScope, session_id: str) -> dict[str, Any]:
        session_ids([session_id])
        async with asyncio.timeout(3), self.transactions.open() as db:
            observed_at = await self._snapshot(db)
            summaries = await self._summaries(db, scope, (session_id,))
            if not summaries:
                raise NotFound
            # Session ownership is established before reading any child evidence.
            attempts_query = (
                select(
                    runs.c.id.label("run_id"),
                    runs.c.session_version,
                    runs.c.status,
                    jobs.c.status.label("job_status"),
                    jobs.c.available_at,
                    jobs.c.lease_until,
                    jobs.c.attempt.label("job_attempt"),
                    model_calls.c.invocation_id,
                    model_calls.c.status.label("model_call_status"),
                    model_calls.c.created_at_utc.label("model_call_created_at"),
                    model_calls.c.created_at.label("legacy_model_call_created_at"),
                )
                .select_from(
                    runs.outerjoin(jobs, jobs.c.run_id == runs.c.id).outerjoin(
                        model_calls, model_calls.c.run_id == runs.c.id
                    )
                )
                .where(runs.c.session_id == session_id)
                .order_by(runs.c.session_version.desc(), runs.c.id)
                .limit(101)
            )
            attempts = []
            for row in (await db.execute(attempts_query)).mappings():
                fields = dict(row)
                legacy = fields.pop("legacy_model_call_created_at")
                item = value(fields)
                item["model_call_time_basis"] = (
                    "utc"
                    if item["model_call_created_at"] is not None
                    else "legacy_timezone_unrecorded"
                    if legacy is not None
                    else "not_recorded"
                )
                item["model_call_created_at_recorded"] = (
                    legacy.isoformat()
                    if legacy is not None and item["model_call_created_at"] is None
                    else None
                )
                attempts.append(item)
            deliveries_query = (
                select(
                    result_outbox.c.event_id,
                    result_outbox.c.version,
                    result_outbox.c.delivered,
                    result_outbox.c.attempts,
                    result_outbox.c.created_at,
                    result_outbox.c.delivered_at,
                    result_outbox.c.available_at,
                )
                .where(result_outbox.c.session_id == session_id)
                .order_by(result_outbox.c.version.desc())
                .limit(101)
            )
            deliveries = [value(row) for row in (await db.execute(deliveries_query)).mappings()]
            milestone_query = (
                select(
                    runtime_milestones.c.dedupe_key,
                    runtime_milestones.c.kind,
                    runtime_milestones.c.run_id,
                    runtime_milestones.c.invocation_id,
                    runtime_milestones.c.attempt,
                    runtime_milestones.c.occurred_at,
                )
                .where(runtime_milestones.c.session_id == session_id)
                .order_by(runtime_milestones.c.occurred_at.desc(), runtime_milestones.c.dedupe_key)
                .limit(301)
            )
            milestones = [value(row) for row in (await db.execute(milestone_query)).mappings()]

        current = summaries[0]
        attempt = next((item for item in attempts if item["run_id"] == current["run_id"]), {})
        can_retry = retry_available(
            current["status"],
            attempt.get("status"),
            attempt.get("job_status"),
            current["publication_id"] is not None,
        )
        return {
            "observed_at": observed_at,
            "actions": [
                {
                    "kind": "retry",
                    "available": can_retry,
                    "reason": "confirmation_and_authorization_required"
                    if can_retry
                    else "current_state_not_retryable",
                    "expected_version": current["version"],
                    "expected_run_id": current["run_id"],
                    "new_model_calls": 1,
                    "unknown_result_risk": current["model_call_status"]
                    in {"unknown", "dispatched"},
                }
            ],
            "execution": current,
            "attempts": attempts[:100],
            "attempts_truncated": len(attempts) > 100,
            "deliveries": deliveries[:100],
            "deliveries_truncated": len(deliveries) > 100,
            "milestones": milestones[:300],
            "milestones_truncated": len(milestones) > 300,
            "history_complete": False,
            "history_note": "Older milestones may be absent; only retained evidence is shown.",
        }

    async def health(self, scope: DraftScope) -> dict[str, Any]:
        async with asyncio.timeout(2), self.transactions.open() as db:
            observed_at = await self._snapshot(db)
            queries = {
                "queued_jobs": (
                    "FROM execution_jobs j JOIN interpretation_sessions s ON s.id=j.session_id "
                    "WHERE s.org_id=:org AND j.status='queued' "
                ),
                "expired_leases": (
                    "FROM execution_jobs j JOIN interpretation_sessions s ON s.id=j.session_id "
                    "WHERE s.org_id=:org AND j.status='leased' AND "
                    "j.lease_until<=UTC_TIMESTAMP(6) "
                ),
                "pending_deliveries": (
                    "FROM result_outbox o JOIN interpretation_sessions s ON s.id=o.session_id "
                    "WHERE s.org_id=:org AND o.delivered=0 "
                ),
                "unknown_model_results": (
                    "FROM model_calls m JOIN interpretation_sessions s ON "
                    "s.active_run_id=m.run_id WHERE s.org_id=:org AND m.status IN "
                    "('unknown','dispatched') AND s.status='blocked' "
                ),
            }
            counts = {
                key: await db.scalar(
                    text("SELECT /*+ MAX_EXECUTION_TIME(1000) */ COUNT(*) " + query),
                    {"org": scope.organization_id},
                )
                for key, query in queries.items()
            }
        return {
            "observed_at": observed_at,
            "availability": "available",
            "partial": False,
            "backlog": counts,
        }
