"""Frozen technical identity is borrowed by the same host-owned Inbox write."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.durable import MessageConflict
from sqlalchemy.dialects import mysql

from qs_ai.infrastructure.persistence.mysql import messaging
from tests.test_transaction_command import command_fixture


@pytest.mark.parametrize("frozen", [False, True])
async def test_inbox_can_borrow_identity_without_allocating_inside_original_root(
    monkeypatch, frozen
):
    envelope, body, wire = command_fixture()
    token = str(uuid4())  # allocated by the host before its original SQL root
    allocate = Mock(return_value=token)
    if frozen:
        allocate.side_effect = AssertionError("frozen Inbox must not allocate inside the root")
    monkeypatch.setattr(messaging, "uuid4", allocate)
    original = SimpleNamespace(append=AsyncMock())
    monkeypatch.setattr(messaging, "bind", lambda _: original)
    row = {
        "body_sha256": envelope.body_sha256,
        "body": body,
        "kind": envelope.kind,
        "aggregate_key": envelope.aggregate_key,
        "reservation_token": token,
    }
    result = Mock()
    result.mappings.return_value.one.return_value = row
    db = SimpleNamespace(execute=AsyncMock(return_value=result))
    store = messaging.MessagingStore()
    if frozen:
        assert (
            await store.reserve_command(db, envelope, body, wire, reservation_token=token) is None
        )
        allocate.assert_not_called()
    else:
        assert await store.reserve_command(db, envelope, body, wire) is None
        allocate.assert_called_once_with()
    statement = original.append.await_args.args[0]
    assert statement.compile(dialect=mysql.dialect()).params["reservation_token"] == token


async def test_invalid_frozen_inbox_identity_is_refused_before_sql_or_replacement(monkeypatch):
    envelope, body, wire = command_fixture()
    original = Mock(side_effect=AssertionError("invalid identity must not borrow a transaction"))
    monkeypatch.setattr(messaging, "bind", original)
    allocate = Mock(side_effect=AssertionError("invalid identity must not be replaced"))
    monkeypatch.setattr(messaging, "uuid4", allocate)
    with pytest.raises(MessageConflict):
        await messaging.MessagingStore().reserve_command(
            Mock(), envelope, body, wire, reservation_token="not-a-canonical-uuid"
        )
    original.assert_not_called()
    allocate.assert_not_called()
