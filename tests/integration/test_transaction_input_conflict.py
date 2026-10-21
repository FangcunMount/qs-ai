"""Exact primary identity/input reads classify conflicts independently of SDK exceptions."""

from dataclasses import replace

import pytest
from reliable_messaging.transaction import BindingError, SchemaError
from reliable_messaging.transaction_mysql import SCHEMA_VERSION
from sqlalchemy import update

from qs_ai.infrastructure.persistence.mysql.transaction_input import input_conflicts
from qs_ai.infrastructure.persistence.mysql.transaction_schema import binding
from qs_ai.infrastructure.persistence.mysql.transaction_schema import request as requests
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.test_review_state_plan import fixture as plan_fixture

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("case", ["same", "input", "store", "identity", "schema"])
async def test_known_input_conflict_requires_original_store_schema_and_exact_request_identity(
    host, case
):
    f = plan_fixture(1)
    request = f["incoming"].intent.request
    reserved = await host.store.reserve(request, f["winner"])
    assert reserved.acquired
    host.executions.append(reserved.execution)
    static = host.store.binding
    changed = replace(request, input_hash=b"different".ljust(32, b"x"))
    try:
        if case == "identity":
            async with host.tx.open() as db, db.begin():
                await db.execute(
                    update(requests)
                    .where(requests.c.request_key == request.key_digest)
                    .values(scope=b"org:wrong")
                )
        elif case == "schema":
            async with host.tx.open() as db, db.begin():
                await db.execute(
                    update(binding)
                    .where(binding.c.owner == static.owner.encode())
                    .values(schema_version=SCHEMA_VERSION + 1)
                )
        elif case == "store":
            static = replace(static, store_id="wrong-static-store")
        if case in ("identity", "schema", "store"):
            with pytest.raises((BindingError, SchemaError)):
                await input_conflicts(host.tx, static, changed)
        elif case == "input":
            with pytest.raises(ValueError):
                await host.store.lookup(changed)
            assert await input_conflicts(host.tx, static, changed) is True
        else:
            assert await input_conflicts(host.tx, static, request) is False
    finally:
        async with host.tx.open() as db, db.begin():
            await db.execute(
                update(requests)
                .where(requests.c.request_key == request.key_digest)
                .values(scope=request.scope.encode())
            )
            await db.execute(
                update(binding)
                .where(binding.c.owner == host.store.binding.owner.encode())
                .values(schema_version=SCHEMA_VERSION)
            )
