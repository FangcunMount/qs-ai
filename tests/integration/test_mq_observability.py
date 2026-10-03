"""Only isolated committed states; no reader runs a task or constructs a message."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, text, update

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import inbox, outbox, quarantine
from qs_ai.infrastructure.persistence.mysql.messaging_observability import (
    collect_messaging_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.metrics import MySQLOperationalMetrics
from qs_ai.transport.http.metrics import render_metrics

pytestmark = pytest.mark.integration


@pytest.fixture
async def observations():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.fail("Required disposable metrics MySQL DSN is missing")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    ids = [str(uuid4()) for _ in range(8)]
    org = uuid4().int % (2**63 - 2) + 1
    try:
        yield transactions, org, ids
    finally:
        async with transactions.open() as db:
            await db.execute(delete(outbox).where(outbox.c.message_id.in_(ids)))
            await db.execute(delete(inbox).where(inbox.c.message_id.in_(ids)))
            await db.execute(delete(quarantine).where(quarantine.c.wire_sha256.in_(ids)))
            await db.commit()
        await database.close()


def record(id, org, stage, created_at, available_at):
    return {
        "producer": "qs-ai",
        "destination": "qs-server",
        "message_id": id,
        "body_sha256": "a" * 64,
        "body": b"synthetic state fixture; not delivered",
        "wire": b"synthetic state fixture; not delivered",
        "wire_sha256": "b" * 64,
        "topic": "qs.ai.events.v1",
        "kind": 7,
        "organization_id": org,
        "aggregate_key": id,
        "aggregate_sequence": 1,
        "ordered": False,
        "requires_receipt": True,
        "stage": stage,
        "available_at": available_at,
        "created_at": created_at,
    }


async def snapshot(transactions, at, org=None):
    async with transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        await db.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        return await collect_messaging_snapshot(db, at, organization_id=org)


async def test_mq_snapshot_distinguishes_due_confirmation_hold_and_org(observations):
    tx, org, ids = observations
    now = datetime.now(UTC).replace(tzinfo=None)
    past, future = now - timedelta(minutes=5), now + timedelta(minutes=1)
    async with tx.open() as db:
        await db.execute(
            insert(outbox),
            [
                record(ids[0], org, "staged", past, past),
                record(ids[1], org, "staged", future, future),
                record(ids[2], org, "awaiting_receipt", past, past),
                record(ids[3], org, "held", past, past),
                record(ids[4], org, "confirmed", past, past),
                record(ids[5], org + 1, "staged", now - timedelta(days=5), past),
                record(ids[6], org + 2, "staged", future, future),
            ],
        )
        await db.commit()
    observed = await snapshot(tx, now, org)
    assert observed == {
        "mq_staged_events": 2,
        "mq_due_events": 2,
        "mq_awaiting_receipt_events": 1,
        "mq_held_events": 1,
        "mq_oldest_staged_seconds": 300,
        "mq_oldest_awaiting_receipt_seconds": 300,
    }
    future_only = await snapshot(tx, now, org + 2)
    assert future_only.pop("mq_staged_events") == 1
    assert all(value == 0 for value in future_only.values())
    assert not any("quarantine" in name or "commands" in name for name in observed)


async def test_mq_snapshot_sees_only_commit_and_never_settles_or_waits_for_writer(observations):
    tx, org, ids = observations
    now = datetime.now(UTC).replace(tzinfo=None)
    async with tx.open() as db:
        await db.execute(insert(outbox).values(**record(ids[0], org, "staged", now, now)))
        assert (await snapshot(tx, now, org))["mq_staged_events"] == 0
        await db.commit()
    assert (await snapshot(tx, now, org))["mq_staged_events"] == 1
    async with tx.open() as writer:
        await writer.execute(
            update(outbox).where(outbox.c.message_id == ids[0]).values(stage="confirmed")
        )
        assert (await snapshot(tx, now, org))["mq_staged_events"] == 1
        await writer.rollback()
    assert (await snapshot(tx, now, org))["mq_staged_events"] == 1
    # Another reader instance observes the same persistent facts, with no flush task.
    assert (await snapshot(tx, now, org))["mq_due_events"] == 1


async def test_mq_security_records_are_gauges_without_invented_duplicate_history(observations):
    tx, org, ids = observations
    now = datetime.now(UTC).replace(tzinfo=None)
    baseline = await snapshot(tx, now)
    async with tx.open() as db:
        await db.execute(
            insert(quarantine).values(
                wire_sha256=ids[0],
                wire=b"untrusted synthetic fixture",
                code="identity_conflict",
                attempts=8,
                first_seen_at=now,
                last_seen_at=now,
            )
        )
        await db.commit()
    observed = await snapshot(tx, now)
    assert observed["mq_quarantine_identity_conflict_records"] == (
        baseline["mq_quarantine_identity_conflict_records"] + 1
    )
    assert observed["mq_duplicate_observations_available"] == 1
    assert observed["mq_payload_error_observations_available"] == 1
    assert observed["mq_observations_history_complete"] == 0
    text_output = render_metrics({**observed, "testee_id": float(org)})
    assert "testee_id" not in text_output
    assert "# TYPE qs_ai_mq_quarantine_identity_conflict_records gauge" in text_output
    assert "duplicate_commands " not in text_output
    assert "payload_errors " not in text_output
    assert "quarantine" not in str(await snapshot(tx, now, org))


async def test_mq_snapshot_failure_discards_core_and_partial_mq_values(observations, monkeypatch):
    tx, _, _ = observations
    settings = Settings(
        _env_file=None,
        messaging={
            "enabled": True,
            "nsqd": {"127.0.0.1:4150": "http://127.0.0.1:4151"},
            "signing_key_file": "inert-not-read",
            "decrypt_key_files": {"inert": "inert-not-read"},
            "qs_signer_files": {"inert": "inert-not-read"},
            "qs_recipient_key_file": "inert-not-read",
        },
    )

    async def unavailable(db, observed_at):
        raise RuntimeError("private DSN and report body must never be exposed")

    monkeypatch.setattr(
        "qs_ai.infrastructure.persistence.mysql.metrics.collect_messaging_snapshot", unavailable
    )
    values = await MySQLOperationalMetrics(tx, settings).collect()
    assert values == {"database_up": 0, "mq_enabled": 1, "mq_observation_available": 0}
    assert "private" not in render_metrics(values)
    assert "staged_events" not in render_metrics(values)
