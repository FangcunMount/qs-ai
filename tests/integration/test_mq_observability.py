"""Only isolated committed states; no reader runs a task or constructs a message."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, text, update

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import inbox, quarantine
from qs_ai.infrastructure.persistence.mysql.messaging_observability import (
    collect_messaging_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.metrics import MySQLOperationalMetrics
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox, sessions
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
    async with transactions.open() as db:
        await db.execute(
            insert(sessions),
            [
                dict(
                    id=id,
                    org_id=org + (1 if i == 5 else 2 if i == 6 else 0),
                    owner_subject_id="fixture",
                    testee_id=1,
                    assessment_ids=[],
                    goal="fixture",
                    status="created",
                    version=1,
                    workflow_version="fixture",
                )
                for i, id in enumerate(ids)
            ],
        )
        await db.commit()
    try:
        yield transactions, org, ids
    finally:
        async with transactions.open() as db:
            await db.execute(delete(result_outbox).where(result_outbox.c.session_id.in_(ids)))
            await db.execute(delete(sessions).where(sessions.c.id.in_(ids)))
            await db.execute(delete(inbox).where(inbox.c.message_id.in_(ids)))
            await db.execute(delete(quarantine).where(quarantine.c.wire_sha256.in_(ids)))
            await db.commit()
        await database.close()


def record(id, delivered, created_at, *, mq_owned=True):
    return dict(
        event_id=id,
        session_id=id,
        version=1,
        payload={},
        delivered=delivered,
        mq_owned=mq_owned,
        created_at=created_at,
        available_at=created_at,
    )


async def snapshot(transactions, at, org=None):
    async with transactions.open() as db:
        await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        await db.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        return await collect_messaging_snapshot(db, at, organization_id=org)


async def test_mq_snapshot_distinguishes_business_ack_ownership_and_org(observations):
    tx, org, ids = observations
    now = datetime.now(UTC).replace(tzinfo=None)
    past, future = now - timedelta(minutes=5), now + timedelta(minutes=1)
    async with tx.open() as db:
        await db.execute(
            insert(result_outbox),
            [
                record(ids[0], False, past),
                record(ids[1], False, future),
                record(ids[2], True, past),
                record(ids[3], False, past, mq_owned=False),
                record(ids[5], False, past),
                record(ids[6], False, future),
            ],
        )
        await db.commit()
    observed = await snapshot(tx, now, org)
    assert observed == {
        "mq_pending_interpretation_receipts": 2,
        "mq_oldest_pending_interpretation_receipt_seconds": 300,
    }
    assert await snapshot(tx, now, org + 2) == {
        "mq_pending_interpretation_receipts": 1,
        "mq_oldest_pending_interpretation_receipt_seconds": 0,
    }
    assert not any("quarantine" in name or "commands" in name for name in observed)


async def test_mq_snapshot_sees_only_commit_and_never_settles_or_waits_for_writer(observations):
    tx, org, ids = observations
    now = datetime.now(UTC).replace(tzinfo=None)
    async with tx.open() as db:
        await db.execute(insert(result_outbox).values(**record(ids[0], False, now)))
        assert (await snapshot(tx, now, org))["mq_pending_interpretation_receipts"] == 0
        await db.commit()
    assert (await snapshot(tx, now, org))["mq_pending_interpretation_receipts"] == 1
    async with tx.open() as writer:
        await writer.execute(
            update(result_outbox).where(result_outbox.c.event_id == ids[0]).values(delivered=True)
        )
        assert (await snapshot(tx, now, org))["mq_pending_interpretation_receipts"] == 1
        await writer.commit()
    assert (await snapshot(tx, now, org))["mq_pending_interpretation_receipts"] == 0


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
