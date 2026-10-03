"""Original organization Health shares only committed, trusted Outbox aggregates."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert

from qs_ai.application.execution.runtime import RuntimeReader
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.runtime import MySQLRuntimeReader
from tests.integration.test_mq_observability import observations as observations
from tests.integration.test_mq_observability import record

pytestmark = pytest.mark.integration


def settings(enabled):
    return Settings(
        _env_file=None,
        database_url=None,
        messaging={
            "enabled": enabled,
            "nsqd": {"127.0.0.1:4150": "http://127.0.0.1:4151"},
            "signing_key_file": "inert-not-read",
            "decrypt_key_files": {"inert": "inert-not-read"},
            "qs_signer_files": {"inert": "inert-not-read"},
            "qs_recipient_key_file": "inert-not-read",
        },
    )


async def test_mq_health_exposes_only_committed_organization_outbox(observations):
    tx, org, ids = observations
    now = datetime.now(UTC).replace(tzinfo=None)
    past = now - timedelta(seconds=30)
    reader = MySQLRuntimeReader(tx, settings(True))
    scope = DraftScope(org, 42)
    async with tx.open() as db:
        await db.execute(
            insert(outbox),
            [
                record(ids[0], org, "staged", past, past),
                record(ids[1], org, "awaiting_receipt", past, past),
                record(ids[2], org, "held", past, past),
                record(ids[3], org + 1, "staged", past, past),
            ],
        )
        before = await reader.health(scope)
        assert before["messaging"]["staged_events"] == 0
        await db.commit()
    observed = await reader.health(scope)
    messaging = observed["messaging"]
    assert messaging["enabled"] and messaging["availability"] == "available"
    assert messaging["observed_at"] == observed["observed_at"]
    assert messaging["staged_events"] == 1
    assert messaging["awaiting_receipt_events"] == 1
    assert messaging["held_events"] == 1
    assert messaging["due_events"] == 2
    assert 30 <= messaging["oldest_staged_seconds"] < 60
    assert set(messaging) == {
        "enabled",
        "availability",
        "observed_at",
        "staged_events",
        "due_events",
        "awaiting_receipt_events",
        "held_events",
        "oldest_staged_seconds",
        "oldest_awaiting_receipt_seconds",
    }
    # Synthetic MQ state is not added to/redefined as old pending deliveries.
    assert all(value == 0 for value in observed["backlog"].values())
    foreign = await reader.health(DraftScope(org + 2, 42))
    assert foreign["messaging"]["staged_events"] == 0
    assert foreign["messaging"]["held_events"] == 0


async def test_mq_health_disabled_never_reads_messaging_tables(observations, monkeypatch):
    tx, org, _ = observations

    async def must_not_read(*args, **kwargs):
        raise AssertionError("Disabled health must not read MQ storage")

    monkeypatch.setattr(
        "qs_ai.infrastructure.persistence.mysql.runtime.collect_messaging_snapshot", must_not_read
    )
    value = await MySQLRuntimeReader(tx, settings(False)).health(DraftScope(org, 42))
    assert value["messaging"] == {"enabled": False, "availability": "disabled"}
    assert value["availability"] == "available" and value["partial"] is False


async def test_mq_health_failure_propagates_without_partial_or_global_counts(
    observations, monkeypatch
):
    tx, org, _ = observations

    async def unavailable(*args, **kwargs):
        raise RuntimeError("private storage details")

    monkeypatch.setattr(
        "qs_ai.infrastructure.persistence.mysql.runtime.collect_messaging_snapshot", unavailable
    )
    # Original gRPC error boundary remains responsible for sanitized UNAVAILABLE.
    with pytest.raises(RuntimeError):
        await MySQLRuntimeReader(tx, settings(True)).health(DraftScope(org, 42))


async def test_mq_health_reader_uses_original_di_without_bootstrap_changes():
    container = create_container(settings(True))
    try:
        async with container() as request:
            reader = await request.get(RuntimeReader)
            assert isinstance(reader, MySQLRuntimeReader)
            assert reader.messaging_enabled
            assert reader.transactions.database.engine is None
    finally:
        await container.close()
