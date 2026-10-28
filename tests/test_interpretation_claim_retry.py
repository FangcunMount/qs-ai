"""Explicit host retry reuses first plan; duplicate lookup never authorizes it."""

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
from qs_ai.infrastructure.workflow_transport.interpretation_claim_sender import (
    InterpretationClaimPending,
    InterpretationClaimSender,
)
from tests import test_interpretation_claim_state as cases
from tests.route_fixtures import legacy_routes

frozen = cases.frozen


@pytest.fixture
async def original(frozen):
    p = frozen
    previous = Execution(p.plan, 1, "m3-1", bytes(32), datetime.now(UTC) + timedelta(seconds=60))
    current = replace(previous, fence=2, token=b"x" * 32)
    runtime = SimpleNamespace(
        store=SimpleNamespace(lookup=AsyncMock()),
        reserve=AsyncMock(),
        execute=AsyncMock(),
        retry=AsyncMock(return_value=Reservation(current, True, State.PREPARED)),
        routing=legacy_routes(),
    )
    sender = InterpretationClaimSender(p.store, SimpleNamespace(), runtime, **p.keys)
    sender._planner.source = AsyncMock(return_value=p.source)
    sender._planner.candidate = AsyncMock()
    sender._execute_owned = AsyncMock(return_value=object())
    return SimpleNamespace(**locals())


async def test_explicit_retry_uses_first_plan_and_new_capability_without_resealing(original):
    p = original
    result = await p.sender.retry(p.p.source, 30, p.previous)
    assert result is p.sender._execute_owned.return_value
    p.runtime.retry.assert_awaited_once_with(RetryAuthorization(p.previous))
    p.sender._execute_owned.assert_awaited_once_with(p.p.source, 30, p.current)
    assert p.current.plan is p.previous.plan and p.current.references != p.previous.references
    p.sender._planner.candidate.assert_not_called()
    p.runtime.reserve.assert_not_called()
    p.runtime.store.lookup.assert_not_called()


async def test_changed_source_cannot_turn_old_abort_into_a_new_claim(original):
    p = original
    p.sender._planner.source.return_value = replace(p.p.source, source_hash=b"x" * 32)
    with pytest.raises(LeaseLost):
        await p.sender.retry(p.p.source, 30, p.previous)
    p.runtime.retry.assert_not_called()
    p.sender._execute_owned.assert_not_called()


async def test_mismatched_first_input_refuses_retry_before_original_source_read(original):
    p = original
    with pytest.raises(ValueError):
        await p.sender.retry(p.p.source, 31, p.previous)
    p.sender._planner.source.assert_not_called()
    p.runtime.retry.assert_not_called()


async def test_store_declining_exact_old_abort_never_executes_local_claim(original):
    p = original
    p.runtime.retry.side_effect = StaleExecutionError("previous execution not exactly aborted")
    with pytest.raises(StaleExecutionError):
        await p.sender.retry(p.p.source, 30, p.previous)
    p.sender._execute_owned.assert_not_called()
    p.sender._planner.candidate.assert_not_called()


async def test_concurrent_retry_loser_cannot_handoff_a_model(original):
    p = original
    p.runtime.retry.return_value = Reservation(p.current, False, State.PREPARED)
    with pytest.raises(InterpretationClaimPending):
        await p.sender.retry(p.p.source, 30, p.previous)
    p.sender._execute_owned.assert_not_called()
