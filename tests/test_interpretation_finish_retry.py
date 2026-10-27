"""Explicit deterministic-finalization retry never repeats a model or first wire."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from reliable_messaging.transaction import (
    Execution,
    Reservation,
    RetryAuthorization,
    StaleExecutionError,
    State,
)

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.infrastructure.workflow_transport import interpretation_finish_sender as sending
from qs_ai.infrastructure.workflow_transport.interpretation_finish_sender import (
    InterpretationFinishPending,
    InterpretationFinishSender,
)
from tests import test_interpretation_finish_state as cases
from tests.route_fixtures import legacy_routes

frozen = cases.frozen


@pytest.fixture
async def original(frozen, monkeypatch):
    p = frozen
    previous = Execution(p.plan, 1, "m3-1", bytes(32), datetime.now(UTC) + timedelta(seconds=60))
    current = replace(previous, fence=2, token=b"x" * 32)

    async def retry(authorization):
        assert not p.opened, "Readonly source must close before public SDK admission"
        assert authorization == RetryAuthorization(previous)
        return Reservation(current, True, State.PREPARED)

    runtime = SimpleNamespace(
        store=SimpleNamespace(lookup=AsyncMock()),
        reserve=AsyncMock(),
        execute=AsyncMock(),
        retry=AsyncMock(side_effect=retry),
        routing=legacy_routes(),
    )
    sender = InterpretationFinishSender(p.store, SimpleNamespace(), runtime, **p.f.keys)
    sender._planner.candidate = AsyncMock(side_effect=AssertionError("Retry must not Seal"))
    sender._execute_owned = AsyncMock()
    monkeypatch.setattr(sending, "finish_request_id", AsyncMock(return_value=p.intent.request_id))
    return SimpleNamespace(**locals())


async def test_explicit_finish_retry_closes_source_and_reuses_first_frozen_result(original):
    p = original
    await p.sender.retry(p.p.incoming.claim, p.p.incoming.result, p.previous)
    p.runtime.retry.assert_awaited_once_with(RetryAuthorization(p.previous))
    p.sender._execute_owned.assert_awaited_once_with(p.p.incoming, p.p.intent, p.current)
    p.sender._planner.candidate.assert_not_called()
    p.runtime.reserve.assert_not_called()
    p.runtime.store.lookup.assert_not_called()


@pytest.mark.parametrize("changed", ["hash", "sequence", "request"])
async def test_changed_original_source_refuses_advance_before_sdk_admission(original, changed):
    p = original
    if changed == "hash":
        p.p.source.return_value = (p.p.incoming.claim.session, "b" * 64, 3)
    elif changed == "sequence":
        p.p.source.return_value = (p.p.incoming.claim.session, "a" * 64, 4)
    else:
        p.p.db.scalar.return_value = "another-original-request"
    with pytest.raises((LeaseLost, ValueError)):
        await p.sender.retry(p.p.incoming.claim, p.p.incoming.result, p.previous)
    p.runtime.retry.assert_not_called()
    p.sender._execute_owned.assert_not_called()


async def test_different_result_cannot_reuse_first_finalization(original):
    p = original
    with pytest.raises(ValueError):
        await p.sender.retry(
            p.p.incoming.claim, replace(p.p.incoming.result, question="changed"), p.previous
        )
    p.runtime.retry.assert_not_called()
    p.sender._execute_owned.assert_not_called()


async def test_non_aborted_or_stale_execution_never_runs_local_finish(original):
    p = original
    p.runtime.retry.side_effect = StaleExecutionError("exact old abort absent")
    with pytest.raises(StaleExecutionError):
        await p.sender.retry(p.p.incoming.claim, p.p.incoming.result, p.previous)
    p.sender._execute_owned.assert_not_called()
    p.sender._planner.candidate.assert_not_called()


async def test_losing_finish_retry_does_not_execute_business(original):
    p = original
    p.runtime.retry.side_effect = None
    p.runtime.retry.return_value = Reservation(p.current, False, State.PREPARED)
    with pytest.raises(InterpretationFinishPending):
        await p.sender.retry(p.p.incoming.claim, p.p.incoming.result, p.previous)
    p.sender._execute_owned.assert_not_called()


async def test_host_resume_explicitly_authorizes_only_exact_aborted_original_finish(original):
    p = original
    p.runtime.store.lookup.return_value = SimpleNamespace(
        acquired=False, state=State.ABORTED, execution=p.previous
    )
    await p.sender.resume(p.p.incoming.claim, p.p.incoming.result)
    p.runtime.retry.assert_awaited_once_with(RetryAuthorization(p.previous))
    p.sender._execute_owned.assert_awaited_once_with(p.p.incoming, p.p.intent, p.current)
    p.runtime.reserve.assert_not_called()
    p.sender._planner.candidate.assert_not_called()


async def test_host_resume_pending_execution_does_not_authorize_new_fence(original):
    p = original
    p.runtime.store.lookup.return_value = SimpleNamespace(
        acquired=False, state=State.PREPARED, execution=p.previous
    )
    with pytest.raises(InterpretationFinishPending):
        await p.sender.resume(p.p.incoming.claim, p.p.incoming.result)
    p.runtime.retry.assert_not_called()
    p.sender._execute_owned.assert_not_called()


async def test_host_resume_rechecks_original_source_before_retry_admission(original):
    p = original
    p.runtime.store.lookup.return_value = SimpleNamespace(
        acquired=False, state=State.ABORTED, execution=p.previous
    )
    p.p.source.return_value = (p.p.incoming.claim.session, "b" * 64, 3)
    with pytest.raises(LeaseLost):
        await p.sender.resume(p.p.incoming.claim, p.p.incoming.result)
    p.runtime.retry.assert_not_called()
    p.sender._execute_owned.assert_not_called()


async def test_host_resume_committed_only_reads_original_witness_and_archive(original):
    p = original
    p.runtime.store.lookup.return_value = SimpleNamespace(
        acquired=False, state=State.COMMITTED, execution=p.previous
    )
    p.sender._committed = AsyncMock()
    await p.sender.resume(p.p.incoming.claim, p.p.incoming.result)
    p.sender._committed.assert_awaited_once_with(p.p.incoming, p.p.intent, p.previous)
    p.runtime.retry.assert_not_called()
    p.sender._execute_owned.assert_not_called()
