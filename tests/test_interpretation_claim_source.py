"""Original session/job/lease read and lock order before exposing a readonly source view."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import Claim
from qs_ai.domain.interpretation.model import Actor, Session, Status
from qs_ai.infrastructure.persistence.mysql import execution as module


@pytest.fixture
def original(monkeypatch):
    session = Session(str(uuid4()), Actor("1", "actor:first"), "7", ("42",), "goal")
    session.queue(str(uuid4()))
    session.running()
    claim = Claim(str(uuid4()), session.active_run_id, replace(session), 7, None, False, None)
    job = dict(status="leased", fence_token=7)
    lease = dict(fence=7, active=True)
    order = []

    async def get(uow, session_id, *, lock=False):
        assert session_id == claim.session.id
        order.append(("session", lock))
        return session

    monkeypatch.setattr(module.MySQLUnitOfWork, "get", get)

    async def execute(statement):
        table = statement.get_final_froms()[0]
        order.append((table.name, statement._for_update_arg is not None))
        value = job if table is module.jobs else lease
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: value))

    db = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    store = module.MySQLExecutionStore(object())
    return SimpleNamespace(**locals())


async def test_original_guard_keeps_session_job_lease_lock_order_and_original_session(original):
    p = original
    assert await p.store._guard(p.db, p.claim) is p.session
    assert p.order == [
        ("session", True),
        (module.jobs.name, True),
        (module.execution_leases.name, True),
    ]


@pytest.mark.parametrize("changed", ["status", "version", "job_fence", "lease"])
async def test_original_guard_refuses_lost_business_or_lease_ownership(original, changed):
    p = original
    if changed == "status":
        p.session.status = Status.BLOCKED
    elif changed == "version":
        p.session.version += 1
    elif changed == "job_fence":
        p.job["fence_token"] += 1
    else:
        p.lease["active"] = False
    with pytest.raises(LeaseLost):
        await p.store._guard(p.db, p.claim)


async def test_readonly_claim_source_has_same_rules_without_locks_or_business_writes(original):
    p = original
    assert await p.store.read_claim(p.db, p.claim) is p.session
    assert p.order == [
        ("session", False),
        (module.jobs.name, False),
        (module.execution_leases.name, False),
    ]
    assert p.db.execute.await_count == 2


async def test_readonly_source_cannot_ignore_original_lease_expiry(original):
    p = original
    p.lease["active"] = False
    with pytest.raises(LeaseLost):
        await p.store.read_claim(p.db, p.claim)
