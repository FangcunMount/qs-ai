"""Only exact primary identity/input evidence classifies a reused operation key."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.transaction import Binding, BindingError, Request, SchemaError

from qs_ai.infrastructure.persistence.mysql.transaction_input import input_conflicts


@pytest.mark.parametrize(
    "damage", [None, "input", "missing", "identity", "store", "schema", "owner"]
)
async def test_input_conflict_is_not_inferred_from_error_text_or_damaged_binding(damage):
    binding = Binding("ai.mysql", "original", "m3-1")
    request = Request("ai.mysql", "org:7", "original-key", b"i" * 32)
    actual_binding = {"store_id": b"original", "schema_version": 1}
    row = {
        "owner": b"ai.mysql",
        "scope": b"org:7",
        "idempotency_key": b"original-key",
        "input_hash": b"i" * 32,
    }
    if damage == "input":
        row["input_hash"] = b"x" * 32
    elif damage == "identity":
        row["scope"] = b"org:8"
    elif damage == "store":
        actual_binding["store_id"] = b"wrong"
    elif damage == "schema":
        actual_binding["schema_version"] = 2
    elif damage == "owner":
        request = Request("wrong", "org:7", "original-key", b"i" * 32)
    first, second = Mock(), Mock()
    first.mappings.return_value.one_or_none.return_value = actual_binding
    second.mappings.return_value.one_or_none.return_value = None if damage == "missing" else row
    db = SimpleNamespace(execute=AsyncMock(side_effect=[first, second]), commit=AsyncMock())

    @asynccontextmanager
    async def opened():
        yield db

    @asynccontextmanager
    async def begun():
        yield

    db.begin = begun
    transactions = SimpleNamespace(open=opened)
    if damage in ("identity", "store", "schema", "owner"):
        with pytest.raises((BindingError, SchemaError)):
            await input_conflicts(transactions, binding, request)
    else:
        assert await input_conflicts(transactions, binding, request) is (damage == "input")
    db.commit.assert_not_called()
    assert all(c.args[0].is_select for c in db.execute.await_args_list)
    if damage == "owner":
        db.execute.assert_not_called()
