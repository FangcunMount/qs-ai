"""Original state-root ownership only; actual MySQL/Broker remain separate gates."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.transaction import BindingError, Evidence, Execution, LocalOutcome, State

from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_state_plan import fixture as planning


def fixture():
    f = planning()
    execution = Execution(f.plan, 1, "m3-1", b"x" * 32, datetime.now(UTC) + timedelta(seconds=60))
    trace = []
    original = SimpleNamespace(is_active=True)
    db = SimpleNamespace(
        info={},
        get_transaction=Mock(return_value=original),
        begin=AsyncMock(side_effect=lambda: trace.append("begin")),
        commit=AsyncMock(side_effect=lambda: trace.append("commit")),
        rollback=AsyncMock(side_effect=lambda: trace.append("rollback")),
    )

    @asynccontextmanager
    async def opened():
        trace.append("open")
        try:
            yield db
        finally:
            trace.append("close")

    guard = SimpleNamespace(
        put=AsyncMock(side_effect=lambda _: trace.append("archive")),
        commit_witness=AsyncMock(side_effect=lambda: trace.append("witness")),
    )

    async def bind(original, candidate):
        assert original is db and candidate is execution
        trace.append("guard")
        return guard

    archives = SimpleNamespace(bind=AsyncMock(side_effect=bind))
    store = SimpleNamespace(
        abort=AsyncMock(return_value=True), evidence=AsyncMock(return_value=None)
    )

    async def apply(original, winner):
        assert original is db and winner == f.facts
        assert db.info["state_events"].plan_digest == f.plan.digest
        trace.append("business")

    business = AsyncMock(side_effect=apply)
    root = EvaluationStateRoot(
        SimpleNamespace(open=opened),
        archives,
        store,
        f.intent,
        f.plan,
        business,
        database_seconds=0.01,
        route_resolver=legacy_routes(),
    )
    root._events.flush = AsyncMock(side_effect=lambda _: trace.append("state-proof"))
    return SimpleNamespace(**{**vars(f), **locals()})


async def test_state_executor_uses_one_original_root_and_no_command_inbox_or_commit_hook():
    f = fixture()
    assert not f.trace
    assert await f.root(f.execution) == LocalOutcome.COMMITTED
    assert f.trace == [
        "open",
        "begin",
        "guard",
        "business",
        "state-proof",
        "archive",
        "witness",
        "commit",
        "close",
    ]
    assert "state_events" not in f.db.info
    f.business.assert_awaited_once()
    f.store.abort.assert_not_called()
    with pytest.raises(BindingError):
        await f.root(f.execution)


@pytest.mark.parametrize("damage", ["capability", "plan"])
async def test_wrong_execution_cannot_open_or_adopt_the_original_state_root(damage):
    f = fixture()
    wrong = (
        replace(f.execution, token=bytes(32))
        if damage == "capability"
        else replace(f.execution, plan=replace(f.plan, operation_id="another-operation"))
    )
    with pytest.raises(BindingError):
        await f.root(wrong)
    assert not f.trace
    f.business.assert_not_called()


@pytest.mark.parametrize("durable_abort", [True, False])
async def test_business_failure_requires_real_rollback_and_durable_abort(durable_abort):
    f = fixture()
    f.business.side_effect = RuntimeError("original CAS conflict")
    f.store.abort.return_value = durable_abort
    assert await f.root(f.execution) == (
        LocalOutcome.ABORTED if durable_abort else LocalOutcome.UNKNOWN
    )
    f.db.rollback.assert_awaited_once()
    f.guard.put.assert_not_called()
    f.guard.commit_witness.assert_not_called()
    assert f.business.await_count == 1


@pytest.mark.parametrize("proof", ["committed", "missing", "foreign", "timeout"])
async def test_lost_root_reply_reads_fresh_exact_proof_with_budget_without_business_redo(proof):
    f = fixture()
    f.db.commit.side_effect = ConnectionError("commit reply unavailable")

    async def evidence(reference):
        assert f.trace[-1] == "close"
        if proof == "timeout":
            await asyncio.Event().wait()
        if proof == "missing":
            return None
        if proof == "foreign":
            reference = replace(reference, fence=reference.fence + 1)
        return Evidence(reference, State.COMMITTED, State.COMMITTED)

    f.store.evidence.side_effect = evidence
    result = await asyncio.wait_for(f.root(f.execution), timeout=0.5)
    assert result == (LocalOutcome.COMMITTED if proof == "committed" else LocalOutcome.UNKNOWN)
    assert f.business.await_count == 1
    f.store.abort.assert_not_called()


async def test_premature_business_commit_cannot_transcribe_or_claim_rollback():
    f = fixture()

    async def replace_root(*_):
        f.db.get_transaction.return_value = None

    f.business.side_effect = replace_root
    assert await f.root(f.execution) == LocalOutcome.UNKNOWN
    f.root._events.flush.assert_not_called()
    f.guard.put.assert_not_called()
    f.store.abort.assert_not_called()


async def test_explicit_governance_snapshot_isolation_precedes_original_guard():
    f = fixture()
    f.db.connection = AsyncMock(side_effect=lambda **_: f.trace.append("isolation"))
    root = EvaluationStateRoot(
        f.root._transactions,
        f.archives,
        f.store,
        f.intent,
        f.plan,
        f.business,
        isolation_level="REPEATABLE READ",
        route_resolver=legacy_routes(),
    )
    root._events.flush = AsyncMock(side_effect=lambda _: f.trace.append("state-proof"))
    assert not f.trace
    assert await root(f.execution) == LocalOutcome.COMMITTED
    assert f.trace.index("begin") < f.trace.index("isolation") < f.trace.index("guard")
    f.db.connection.assert_awaited_once_with(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )


def test_original_state_root_rejects_unreviewed_isolation_before_opening():
    f = fixture()
    with pytest.raises(ValueError):
        EvaluationStateRoot(
            f.root._transactions,
            f.archives,
            f.store,
            f.intent,
            f.plan,
            f.business,
            isolation_level="AUTOCOMMIT",
            route_resolver=legacy_routes(),
        )
    assert not f.trace
