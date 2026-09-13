"""Bounded, read-only cutover statistics; never fetch identities or model/report bodies."""

import math
import re
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from qs_ai.infrastructure.persistence.mysql.database import Transactions

# Select the configuration bound to each session, not today's publication pointer.
SELECTED = """WITH selected AS (
 SELECT s.id, s.status, s.created_at
 FROM interpretation_sessions s
 JOIN execution_configurations c ON c.session_id = s.id
 JOIN configuration_publications p ON p.publication_id = c.publication_id
 WHERE s.workflow_version = 'qs-published-snapshot-v1'
 AND JSON_UNQUOTE(JSON_EXTRACT(p.content_json,
 '$.publication.evidence.manifest.profile.fingerprint')) = :profile
) """
COHORT = "s.created_at >= :since AND s.created_at < :until"


def summarize(values: list) -> dict:
    samples = []
    missing = invalid = 0
    for value in values:
        if value is None:
            missing += 1
        else:
            number = float(value)
            if math.isfinite(number) and number >= 0:
                samples.append(number)
            else:
                invalid += 1
    samples.sort()
    return {
        "n": len(samples),
        "missing": missing,
        "invalid": invalid,
        "known_total": sum(samples) if samples else None,
        "p50": samples[math.ceil(len(samples) * 0.5) - 1] if samples else None,
        "p95": samples[math.ceil(len(samples) * 0.95) - 1] if samples else None,
        "max": samples[-1] if samples else None,
    }


async def observe(
    transactions: Transactions, profile: str, since: datetime, until: datetime
) -> dict:
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", profile):
        raise ValueError("Exact profile fingerprint required")
    if any(at.tzinfo is None or at.utcoffset() is None for at in (since, until)):
        raise ValueError("UTC offsets are required")
    since, until = (at.astimezone(UTC).replace(tzinfo=None) for at in (since, until))
    if not timedelta(0) < until - since <= timedelta(days=31):
        raise ValueError("Observation window must be positive and at most 31 days")
    params = {"profile": profile, "since": since, "until": until}
    async with transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        await db.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        now = await db.scalar(text("SELECT UTC_TIMESTAMP(6)"))
        if until > now:
            raise ValueError("Observation end must not be in the future")
        params["now"] = now

        async def rows(sql: str) -> list:
            # Project numeric values/status only; do not return IDs, payloads or credentials.
            sql = sql.replace("SELECT ", "SELECT /*+ MAX_EXECUTION_TIME(10000) */ ", 1)
            result = (
                (await db.execute(text(SELECTED + sql + " LIMIT 100001"), params)).mappings().all()
            )
            if len(result) > 100000:
                raise ValueError("Too many samples; narrow the observation window")
            return list(result)

        states = await rows(
            f"SELECT s.status, COUNT(*) AS n FROM selected s WHERE {COHORT} GROUP BY s.status"
        )
        calls = await rows(f"""SELECT m.status,
            JSON_EXTRACT(m.response_json, '$.latency_milliseconds') AS latency,
            JSON_EXTRACT(m.response_json, '$.input_tokens') AS input_tokens,
            JSON_EXTRACT(m.response_json, '$.output_tokens') AS output_tokens
            FROM selected s JOIN interpretation_runs r ON r.session_id=s.id
            JOIN model_calls m ON m.run_id=r.id WHERE {COHORT}""")
        generation = await rows(f"""SELECT
            TIMESTAMPDIFF(MICROSECOND,s.created_at,a.created_at)/1000 AS ms
            FROM selected s JOIN interpretation_artifacts a ON a.session_id=s.id WHERE {COHORT}""")
        deliveries = await rows(f"""SELECT e.delivered,
            TIMESTAMPDIFF(MICROSECOND,e.created_at,e.delivered_at)/1000 AS delivery_ms,
            CASE WHEN e.created_at IS NOT NULL THEN
                TIMESTAMPDIFF(MICROSECOND,s.created_at,e.delivered_at)/1000 END AS total_ms
            FROM selected s JOIN result_outbox e ON e.session_id=s.id
            WHERE {COHORT} AND JSON_UNQUOTE(JSON_EXTRACT(e.payload,'$.status'))='completed'""")
        # Backlog intentionally includes older sessions of this Profile, beyond the cohort.
        queue = await rows("""SELECT j.status, j.available_at <= :now AS due,
            CASE WHEN j.status='queued' THEN TIMESTAMPDIFF(MICROSECOND,j.available_at,:now)/1000
                 WHEN j.status='leased' THEN
                    TIMESTAMPDIFF(MICROSECOND,j.lease_until,:now)/1000 END AS overdue_ms
            FROM selected s JOIN execution_jobs j ON j.session_id=s.id
            WHERE j.status IN ('queued','leased')""")
        pending = await rows("""SELECT e.attempts,
            TIMESTAMPDIFF(MICROSECOND,e.created_at,:now)/1000 AS age_ms
            FROM selected s JOIN result_outbox e ON e.session_id=s.id WHERE e.delivered=0""")
        connections = await db.scalar(
            text("""SELECT COUNT(*) FROM information_schema.PROCESSLIST
            WHERE USER=SUBSTRING_INDEX(CURRENT_USER(),'@',1) AND DB=DATABASE()""")
        )

    # MySQL JSON scalar extraction can return the JSON text 'null'.
    def numbers(field: str) -> list:
        return [None if r[field] in (None, "null") else r[field] for r in calls]

    call_states: dict[str, int] = {}
    for call in calls:
        call_states[call["status"]] = call_states.get(call["status"], 0) + 1
    acknowledged = [r for r in deliveries if r["delivered"]]
    return {
        "schema_version": "qs-ai-cutover-observation/v1",
        "profile_fingerprint": profile,
        "window": {
            "basis": "session creation cohort; outcomes at observed_at, not historical replay",
            "since_inclusive": since.replace(tzinfo=UTC).isoformat(),
            "until_exclusive": until.replace(tzinfo=UTC).isoformat(),
        },
        "observed_at": now.replace(tzinfo=UTC).isoformat(),
        "quantile_method": "nearest_rank",
        "request_status_counts": {r["status"]: r["n"] for r in states},
        "model_call_status_counts": call_states,
        "provider_full_response_ms": summarize(numbers("latency")),
        "input_tokens": summarize(numbers("input_tokens")),
        "output_tokens": summarize(numbers("output_tokens")),
        "request_to_artifact_ms": summarize([r["ms"] for r in generation]),
        "completion_delivery_ms": summarize([r["delivery_ms"] for r in acknowledged]),
        "request_to_acknowledged_completion_ms": summarize([r["total_ms"] for r in acknowledged]),
        "completion_events_pending": len(deliveries) - len(acknowledged),
        "backlog": {
            "scope": "all bound sessions of this Profile, not limited to the cohort window",
            "queued_ready": sum(r["status"] == "queued" and bool(r["due"]) for r in queue),
            "queued_deferred": sum(r["status"] == "queued" and not r["due"] for r in queue),
            "ready_queue_overdue_ms": summarize(
                [r["overdue_ms"] for r in queue if r["status"] == "queued" and r["due"]]
            ),
            "expired_leases": sum(
                r["status"] == "leased" and r["overdue_ms"] is not None and r["overdue_ms"] >= 0
                for r in queue
            ),
            "pending_result_events": len(pending),
            "pending_result_age_ms": summarize([r["age_ms"] for r in pending]),
            "result_retry_attempts": sum(r["attempts"] for r in pending),
        },
        "database_account_connections_including_observer": connections,
        "acceptance_passed": False,
    }
