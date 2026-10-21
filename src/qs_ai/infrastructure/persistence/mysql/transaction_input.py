"""Exact request-input conflict evidence; never proves local Commit or Rollback.

The host explicitly installs the fixed SDK schema. This short primary read is
used only to classify a rejected key reuse, independently of exception wording.
"""

from reliable_messaging.transaction import Binding, BindingError, Request, SchemaError
from reliable_messaging.transaction_mysql import SCHEMA_VERSION
from sqlalchemy import select

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_schema import binding as bindings
from qs_ai.infrastructure.persistence.mysql.transaction_schema import request as requests


async def input_conflicts(transactions: Transactions, binding: Binding, request: Request) -> bool:
    if request.owner != binding.owner:
        raise BindingError("Request owner differs from original binding")
    async with transactions.open() as db, db.begin():
        actual = (
            (
                await db.execute(
                    select(bindings)
                    .where(bindings.c.owner == binding.owner.encode())
                    .with_for_update(read=True)
                )
            )
            .mappings()
            .one_or_none()
        )
        if actual is None or bytes(actual["store_id"]) != binding.store_id.encode():
            raise BindingError("Original request store differs")
        if actual["schema_version"] != SCHEMA_VERSION:
            raise SchemaError("Original request schema differs")
        row = (
            (await db.execute(select(requests).where(requests.c.request_key == request.key_digest)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return False
        if (bytes(row["owner"]), bytes(row["scope"]), bytes(row["idempotency_key"])) != (
            request.owner.encode(),
            request.scope.encode(),
            request.key.encode(),
        ):
            raise BindingError("Original request identity differs")
        return bytes(row["input_hash"]) != request.input_hash
