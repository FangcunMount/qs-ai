"""Real MySQL aggregates against isolated fixtures, never production fault injection."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import insert, update

from qs_ai.infrastructure.persistence.mysql.metrics import MySQLOperationalMetrics
from qs_ai.infrastructure.persistence.mysql.schema import jobs, model_calls, result_outbox, sessions
from tests.integration.test_interpretation import kit as kit

pytestmark = pytest.mark.integration


async def test_backlog_unknown_call_recovery_and_full_response_metrics(kit):
    reader = MySQLOperationalMetrics(kit.transactions)
    baseline = await reader.collect()
    assert baseline["database_up"] == 1
    receipt = await kit.queued()
    now = datetime.now(UTC).replace(tzinfo=None)
    async with kit.transactions.open() as db:
        await db.execute(
            update(jobs)
            .where(jobs.c.session_id == receipt.session_id)
            .values(available_at=now - timedelta(minutes=4))
        )
        await db.execute(
            insert(result_outbox).values(
                event_id=str(uuid4()),
                session_id=receipt.session_id,
                version=1,
                payload={},
                delivered=False,
                created_at=now - timedelta(minutes=3),
            )
        )
        await db.execute(
            insert(model_calls).values(
                run_id=receipt.run_id,
                invocation_id=str(uuid4()),
                fence_token=0,
                status="unknown",
                request_json="{}",
                created_at=now,
            )
        )
        await db.commit()
    metrics = await reader.collect()
    assert metrics["database_up"] == 1
    assert metrics["ready_jobs"] == baseline["ready_jobs"] + 1
    assert metrics["oldest_ready_job_seconds"] >= 240
    assert metrics["pending_results"] == baseline["pending_results"] + 1
    assert metrics["oldest_pending_result_seconds"] >= 180
    assert metrics["unresolved_model_calls"] == baseline["unresolved_model_calls"] + 1
    assert metrics["database_account_connections"] >= 1

    async with kit.transactions.open() as db:
        await db.execute(
            update(jobs)
            .where(jobs.c.session_id == receipt.session_id)
            .values(status="leased", lease_until=now - timedelta(seconds=10))
        )
        await db.execute(
            update(model_calls)
            .where(model_calls.c.run_id == receipt.run_id)
            .values(status="response_received", response_json='{"latency_milliseconds":1250}')
        )
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.session_id == receipt.session_id)
            .values(delivered=True)
        )
        await db.commit()
    recovered = await reader.collect()
    assert recovered["ready_jobs"] == baseline["ready_jobs"]
    assert recovered["pending_results"] == baseline["pending_results"]
    assert recovered["unresolved_model_calls"] == baseline["unresolved_model_calls"]
    assert recovered["expired_job_leases"] == baseline["expired_job_leases"] + 1
    assert recovered["provider_response_samples_5m"] == baseline["provider_response_samples_5m"] + 1
    assert recovered["provider_response_max_seconds_5m"] >= 1.25

    # A late unknown receipt of a completed request must not alarm forever.
    async with kit.transactions.open() as db:
        await db.execute(
            update(model_calls)
            .where(model_calls.c.run_id == receipt.run_id)
            .values(status="unknown", response_json=None)
        )
        await db.execute(
            update(sessions).where(sessions.c.id == receipt.session_id).values(status="completed")
        )
        await db.commit()
    assert (await reader.collect())["unresolved_model_calls"] == baseline["unresolved_model_calls"]
