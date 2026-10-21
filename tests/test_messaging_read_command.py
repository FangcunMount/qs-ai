"""A compatibility read cannot reserve an Inbox or manufacture a transaction witness."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from tests.test_transaction_command import command_fixture


@pytest.mark.parametrize("state", ["accepted", "rejected", "held", "missing"])
async def test_exact_completed_original_command_is_only_read_without_lock_or_write(state):
    envelope, raw, _ = command_fixture()
    row = (
        None
        if state == "missing"
        else {
            "body_sha256": envelope.body_sha256,
            "body": raw,
            "kind": envelope.kind,
            "aggregate_key": envelope.aggregate_key,
            "decision": state,
            "receipt_id": str(uuid4()),
        }
    )
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    db = SimpleNamespace(execute=AsyncMock(return_value=result), commit=AsyncMock())
    assert await MessagingStore().read_command(db, envelope, raw) is row
    statement = db.execute.await_args.args[0]
    assert statement.is_select and statement._for_update_arg is None
    db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["hash", "body", "kind", "aggregate", "pending", "receipt"])
async def test_conflicting_or_unresolved_legacy_record_never_looks_like_completed_business(damage):
    envelope, raw, _ = command_fixture()
    row = {
        "body_sha256": envelope.body_sha256,
        "body": raw,
        "kind": envelope.kind,
        "aggregate_key": envelope.aggregate_key,
        "decision": "accepted",
        "receipt_id": str(uuid4()),
    }
    field = {
        "hash": "body_sha256",
        "body": "body",
        "kind": "kind",
        "aggregate": "aggregate_key",
        "pending": "decision",
        "receipt": "receipt_id",
    }[damage]
    row[field] = "processing" if damage == "pending" else "" if damage == "receipt" else "wrong"
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    db = SimpleNamespace(execute=AsyncMock(return_value=result))
    with pytest.raises(RuntimeError if damage in ("pending", "receipt") else MessageConflict):
        await MessagingStore().read_command(db, envelope, raw)
