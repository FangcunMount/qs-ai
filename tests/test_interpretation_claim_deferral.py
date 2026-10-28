"""No-event operational delay is separate from SDK execution and model admission."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from reliable_messaging.transaction import Binding, BindingError, Evidence, Execution, State

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.errors import LeaseLost
from qs_ai.domain.interpretation.model import Actor, Session
from qs_ai.infrastructure.workflow_transport import interpretation_claim_deferral as module
from qs_ai.infrastructure.workflow_transport.interpretation_claim_state import (
    InterpretationClaimPlanner,
    InterpretationClaimSource,
)
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_state_plan import fixture as state_fixture


@pytest.fixture
async def original(monkeypatch):
    keys = state_fixture().keys
    session = Session(
        str(uuid4()),
        Actor("7", "actor:first"),
        "8",
        ("9",),
        "goal",
        workflow_version="qs-snapshot-v1",
    )
    session.queue(str(uuid4()))
    source = InterpretationClaimSource(
        session, str(uuid4()), str(uuid4()), 0, 0, hashlib.sha256(b"precise original").digest()
    )
    events, root = [], SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        info={},
        connection=AsyncMock(),
        get_transaction=Mock(return_value=root),
        execute=AsyncMock(),
        commit=AsyncMock(),
    )

    @asynccontextmanager
    async def open():
        events.append("open")
        try:
            yield db
        finally:
            events.append("close")

    business = SimpleNamespace(
        transactions=SimpleNamespace(open=open),
        capacity=ParticipantCapacityPolicy(),
        quota_baseline=None,
        claim_in=AsyncMock(),
    )
    planner = InterpretationClaimPlanner(business, **keys, route_resolver=legacy_routes())
    planner.source = AsyncMock(return_value=source)
    plan = await planner.candidate(source, 60)
    events.clear()
    execution = Execution(plan, 1, "m3-1", b"x" * 32, datetime.now(UTC) + timedelta(seconds=60))
    evidence = Evidence(execution.references[0], State.ABORTED, State.ABORTED)
    installed = dict(store_id=b"original-db", writer_epoch=b"m3-1", schema_version=1)
    db.execute.return_value = SimpleNamespace(
        mappings=lambda: SimpleNamespace(
            one_or_none=lambda: installed,
        )
    )
    store = SimpleNamespace(
        evidence=AsyncMock(return_value=evidence),
        binding=Binding("ai.mysql", "original-db", "m3-1"),
    )
    archives = SimpleNamespace(read_committed=AsyncMock(return_value=None))
    binding = SimpleNamespace(validate=AsyncMock())
    read, acquire = AsyncMock(return_value=source), AsyncMock(return_value=False)
    monkeypatch.setattr(module, "read_claim_source", read)
    monkeypatch.setattr(module, "acquire", acquire)
    monkeypatch.setattr(module, "bind", lambda _: binding)
    deferred = module.InterpretationClaimDeferral(
        business, store, archives, route_resolver=legacy_routes()
    )
    return SimpleNamespace(**locals())


async def test_exact_aborted_candidate_only_defers_still_denied_original_job(original, monkeypatch):
    p = original
    at = datetime(2026, 10, 22, tzinfo=UTC)

    def now(_):
        assert not p.events, "Clock must freeze before operational root opens"
        return at

    monkeypatch.setattr(module, "datetime", SimpleNamespace(now=now))
    assert await p.deferred(p.source, 60, p.execution) is True
    assert p.events == ["open", "close"]
    p.business.claim_in.assert_not_called()
    p.acquire.assert_awaited_once_with(p.db, p.source.session, p.business.capacity, at, None)
    assert p.read.await_args.kwargs == {"lock": True}
    p.db.commit.assert_awaited_once()
    statement = p.db.execute.await_args.args[0]
    assert statement.table is module.jobs
    assert set(statement._values) == {"available_at"}
    assert p.binding.validate.await_count == 2


@pytest.mark.parametrize("damage", ["missing", "prepared", "committed", "wrong_reference"])
async def test_unknown_or_wrong_previous_evidence_cannot_open_a_deferral_root(original, damage):
    p = original
    ref = p.execution.references[0]
    if damage == "wrong_reference":
        evidence = Evidence(replace(ref, fence=2), State.ABORTED, State.ABORTED)
    elif damage == "missing":
        evidence = None
    else:
        evidence = Evidence(ref, State(damage), State(damage))
    p.store.evidence.return_value = evidence
    with pytest.raises(module.InterpretationDeferralPending):
        await p.deferred(p.source, 60, p.execution)
    assert not p.events
    p.acquire.assert_not_called()
    p.business.claim_in.assert_not_called()


async def test_committed_archive_contradiction_is_not_job_deferral_permission(original):
    p = original
    p.archives.read_committed.return_value = object()
    with pytest.raises(module.InterpretationDeferralPending):
        await p.deferred(p.source, 60, p.execution)
    assert not p.events


@pytest.mark.parametrize("changed", [False, True])
async def test_source_changed_or_no_longer_due_leaves_job_untouched(original, changed):
    p = original
    if changed:
        p.read.return_value = replace(p.source, source_hash=b"x" * 32)
    else:
        p.read.side_effect = LeaseLost("original job no longer due")
    assert await p.deferred(p.source, 60, p.execution) is False
    p.acquire.assert_not_called()
    assert p.db.execute.await_count == 1  # Read-only original binding check only.
    p.db.commit.assert_not_called()


async def test_capacity_available_does_not_commit_staged_slot_or_create_a_claim(original):
    p = original
    p.acquire.return_value = True
    assert await p.deferred(p.source, 60, p.execution) is False
    assert p.db.execute.await_count == 1  # Read-only original binding check only.
    p.db.commit.assert_not_called()
    p.business.claim_in.assert_not_called()


async def test_replaced_operational_root_cannot_be_reported_committed(original):
    p = original

    async def acquire(*_):
        p.db.get_transaction.return_value = SimpleNamespace(is_active=True)
        return False

    p.acquire.side_effect = acquire
    with pytest.raises(module.InterpretationDeferralPending):
        await p.deferred(p.source, 60, p.execution)
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("error", [ConnectionError, asyncio.CancelledError])
async def test_lost_job_commit_reply_never_claims_rollback_or_model_authority(original, error):
    p = original
    p.db.commit.side_effect = error()
    with pytest.raises(module.InterpretationDeferralPending):
        await p.deferred(p.source, 60, p.execution)
    p.db.commit.assert_awaited_once()
    p.business.claim_in.assert_not_called()
    assert p.events == ["open", "close"]


@pytest.mark.parametrize("damage", ["store_id", "writer_epoch", "schema_version"])
async def test_wrong_original_database_binding_refuses_job_writes(original, damage):
    p = original
    p.installed[damage] = 2 if damage == "schema_version" else b"wrong"
    with pytest.raises(BindingError):
        await p.deferred(p.source, 60, p.execution)
    p.read.assert_not_called()
    p.acquire.assert_not_called()
    p.db.commit.assert_not_called()
    assert p.db.execute.await_count == 1


async def test_previous_writer_epoch_cannot_authorize_current_job_maintenance(original):
    p = original
    with pytest.raises(BindingError):
        await p.deferred(p.source, 60, replace(p.execution, writer_epoch="old-epoch"))
    assert not p.events
    p.store.evidence.assert_not_called()


async def test_job_only_root_does_not_use_the_legacy_commit_recorder(original):
    p = original
    legacy = SimpleNamespace(flush=AsyncMock(), record_interpretation=AsyncMock())
    database = SimpleNamespace(state_events=legacy)
    p.business.transactions.database = database
    p.db.info["state_events"] = legacy

    async def commit():
        await p.db.info["state_events"].flush(p.db)

    p.db.commit.side_effect = commit
    assert await p.deferred(p.source, 60, p.execution) is True
    legacy.flush.assert_not_called()
    assert database.state_events is legacy


@pytest.mark.parametrize(
    "damage", ["interpretation_write", "evaluation_write", "recorder_replaced"]
)
async def test_job_only_root_refuses_state_writes_or_recorder_replacement_before_commit(
    original, damage
):
    p = original
    legacy = SimpleNamespace(flush=AsyncMock(), record_interpretation=AsyncMock())
    p.db.info["state_events"] = legacy

    async def changed(*args):
        if damage == "interpretation_write":
            await p.db.info["state_events"].record_interpretation(p.db, {})
        elif damage == "evaluation_write":
            p.db.info["evaluation_events"] = {"unexpected-state-change"}
        else:
            p.db.info["state_events"] = legacy
        return False

    p.acquire.side_effect = changed
    with pytest.raises(BindingError):
        await p.deferred(p.source, 60, p.execution)
    p.db.commit.assert_not_called()
    legacy.flush.assert_not_called()


def test_independent_operational_action_refuses_a_borrowed_business_transaction(original):
    from qs_ai.infrastructure.persistence.mysql.database import BorrowedTransactions

    p = original
    p.business.transactions = BorrowedTransactions(p.db)
    with pytest.raises(BindingError):
        module.InterpretationClaimDeferral(
            p.business, p.store, p.archives, route_resolver=legacy_routes()
        )
