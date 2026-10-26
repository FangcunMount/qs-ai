"""ACK dispatch and exact first-receipt contracts; actual MySQL roots tested separately."""

import asyncio
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.infrastructure.persistence.mysql import transaction_receipts as module
from qs_ai.infrastructure.persistence.mysql.transaction_receipts import TransactionMessagingStore
from qs_ai.infrastructure.workflow_transport.frozen_start_events import same_state_event
from qs_ai.infrastructure.workflow_transport.messaging import SCHEMA, MessagingContractError


def acknowledgement(event_id=None, *, outcome=pb.MessagingEventAcknowledgement.STORED):
    body = pb.MessagingBody(
        event_acknowledgement=pb.MessagingEventAcknowledgement(
            event_id=event_id or str(uuid4()),
            event_kind=pb.EVALUATION_STATE,
            event_body_sha256="a" * 64,
            outcome=outcome,
        )
    ).SerializeToString(deterministic=True)
    envelope = pb.MessagingEnvelope(
        schema_version=SCHEMA,
        producer="qs-server",
        destination="qs-ai",
        message_id=str(uuid4()),
        aggregate_key=str(uuid4()),
        kind=pb.EVENT_ACKNOWLEDGEMENT,
        body_sha256=hashlib.sha256(body).hexdigest(),
        body_length=len(body),
        inline_body=body,
        original_occurred_at="2026-10-22T00:00:00Z",
    )
    return envelope, body


def result(value):
    row = Mock()
    row.mappings.return_value.one.return_value = value
    row.mappings.return_value.one_or_none.return_value = value
    return row


@pytest.fixture
def setup(monkeypatch):
    archives, original = Mock(), Mock()
    original.validate = AsyncMock()
    monkeypatch.setattr(module, "bind", lambda db: original)
    archives.read_committed = AsyncMock()
    return SimpleNamespace(
        archives=archives,
        original=original,
        store=TransactionMessagingStore(archives),
        db=SimpleNamespace(
            execute=AsyncMock(),
            get_transaction=lambda: SimpleNamespace(is_active=True),
            get_nested_transaction=lambda: None,
        ),
    )


async def test_absent_identity_rejects_without_legacy_business_confirmation(setup):
    p = setup
    header, raw = acknowledgement()
    p.archives.read_committed.return_value = None
    with pytest.raises(MessageConflict):
        await p.store.confirm_event(p.db, header, raw)
    p.db.execute.assert_not_awaited()


@pytest.mark.parametrize("error", [MessagingContractError("uncommitted"), TimeoutError()])
async def test_unknown_prepared_or_unavailable_archive_never_selects_legacy(setup, error):
    p = setup
    p.archives.read_committed.side_effect = error
    header, raw = acknowledgement()
    with pytest.raises(type(error)):
        await p.store.confirm_event(p.db, header, raw)
    p.db.execute.assert_not_awaited()


@pytest.mark.parametrize("fault", ["missing_root", "inactive", "nested"])
async def test_ack_business_evidence_requires_the_original_root_before_archive_read(setup, fault):
    p = setup
    p.db.get_transaction = lambda: (
        None if fault == "missing_root" else SimpleNamespace(is_active=fault != "inactive")
    )
    p.db.get_nested_transaction = lambda: object() if fault == "nested" else None
    header, raw = acknowledgement()
    with pytest.raises(MessageConflict):
        await p.store.confirm_event(p.db, header, raw)
    p.archives.read_committed.assert_not_awaited()
    p.db.execute.assert_not_awaited()


async def test_archive_database_timeout_preserves_unknown_without_legacy_confirmation(setup):
    p = setup
    cancelled = asyncio.Event()

    async def unavailable(_):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    p.archives.read_committed.side_effect = unavailable
    store = TransactionMessagingStore(p.archives, database_seconds=0.01)
    header, raw = acknowledgement()
    with pytest.raises(TimeoutError):
        await store.confirm_event(p.db, header, raw)
    assert cancelled.is_set()
    p.db.execute.assert_not_awaited()


def current(p, header, raw):
    ack = pb.MessagingBody.FromString(raw).event_acknowledgement
    event = pb.MessagingEnvelope(
        producer="qs-ai",
        destination="qs-server",
        message_id=ack.event_id,
        kind=ack.event_kind,
        aggregate_key=header.aggregate_key,
        body_sha256=ack.event_body_sha256,
    )
    archived = SimpleNamespace(envelope=lambda: event, requires_receipt=True, organization_id=7)
    p.archives.read_committed.return_value = archived
    stored = dict(
        producer=b"qs-server",
        ack_id=header.message_id.encode(),
        event_id=event.message_id.encode(),
        organization_id=7,
        header=header.SerializeToString(deterministic=True),
        body=raw,
        outcome=ack.outcome,
    )
    return archived, event, stored


@pytest.mark.parametrize("outcome", [1, 2])
async def test_new_receipt_is_exact_and_never_stages_legacy_publication(setup, outcome):
    p = setup
    header, raw = acknowledgement(outcome=outcome)
    _, _, stored = current(p, header, raw)
    p.db.execute.side_effect = [result(None), result(stored)]
    await p.store.confirm_event(p.db, header, raw)
    assert p.db.execute.await_count == 2
    assert p.db.execute.await_args_list[0].args[0].table.name == "messaging_event_receipts"
    assert p.original.validate.await_count == 3


@pytest.mark.parametrize("field", ["kind", "aggregate_key", "body_sha256", "requires_receipt"])
async def test_new_archive_mismatch_rejects_without_a_receipt_or_legacy_fallback(setup, field):
    p = setup
    header, raw = acknowledgement()
    archived, event, _ = current(p, header, raw)
    if field == "requires_receipt":
        archived.requires_receipt = False
    elif field == "kind":
        event.kind = pb.COMMAND_RECEIPT
    else:
        setattr(event, field, "changed")
    with pytest.raises(MessageConflict):
        await p.store.confirm_event(p.db, header, raw)
    p.db.execute.assert_not_awaited()


@pytest.mark.parametrize("field", ["event_id", "header", "body", "outcome", "organization_id"])
async def test_reusing_ack_identity_cannot_replace_its_original_business_receipt(setup, field):
    p = setup
    header, raw = acknowledgement()
    _, _, stored = current(p, header, raw)
    stored[field] = 9 if field in ("outcome", "organization_id") else b"changed"
    p.db.execute.side_effect = [result(None), result(stored)]
    with pytest.raises(MessageConflict):
        await p.store.confirm_event(p.db, header, raw)


def test_original_state_comparison_preserves_first_bytes_and_all_other_fields():
    expected = workflow.StateEvent(
        event_id=str(uuid4()), status="completed", artifact_json='{"a":"1","b":"2"}'
    )
    actual = workflow.StateEvent.FromString(expected.SerializeToString())
    actual.artifact_json = '{"b": "2", "a": "1"}'
    before = actual.SerializeToString(), expected.SerializeToString()
    assert same_state_event(actual, expected)
    assert (actual.SerializeToString(), expected.SerializeToString()) == before
    actual.version = expected.version + 1
    assert not same_state_event(actual, expected)
    actual.version = expected.version
    actual.artifact_json = '{"a":"1","a":"1","b":"2"}'
    with pytest.raises(ValueError):
        same_state_event(actual, expected)
