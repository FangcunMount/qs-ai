"""Original completed-claim detection is a no-op read, never a Broker or model decision."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_completion as original
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim


@pytest.fixture(params=[True, False])
def source(monkeypatch, request):
    lock = request.param
    at, run_id = datetime(2026, 10, 22, tzinfo=UTC), uuid4()
    cp = ExecutionCheckpoint(
        "execution:first",
        "generation",
        "case:first",
        1,
        "",
        1,
        "worker:first",
        "invocation:first",
        "prepared",
        at,
        at + timedelta(minutes=5),
    ).mark_dispatching("worker:first", at)
    claim = SlotClaim(run_id, 4, cp)
    ledger, terminal = Mock(), Mock()
    ledger.scalar_one_or_none.return_value = encode(cp)
    terminal.scalar_one_or_none.return_value = cp.execution_id
    db = SimpleNamespace(execute=AsyncMock(side_effect=[ledger, terminal]), commit=AsyncMock())
    coordinator, active = AsyncMock(return_value=({}, 8)), AsyncMock(return_value=())
    monkeypatch.setattr(original, "lock_run", coordinator)
    monkeypatch.setattr(original, "active_claims", active)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize(
    "state",
    [
        "completed",
        "missing_ledger",
        "missing_terminal",
        "foreign_owner",
        "foreign_terminal",
        "renewed",
    ],
)
async def test_original_completed_recovery_keeps_exact_dispatch_and_no_write_authority(
    source, state
):
    p = source
    claim = p.claim
    if state == "missing_ledger":
        p.ledger.scalar_one_or_none.return_value = None
    if state == "missing_terminal":
        p.terminal.scalar_one_or_none.return_value = None
    if state == "foreign_owner":
        p.ledger.scalar_one_or_none.return_value = encode(replace(p.cp, owner="worker:other"))
    if state == "foreign_terminal":
        p.terminal.scalar_one_or_none.return_value = "execution:other"
    if state == "renewed":
        claim = replace(
            claim,
            checkpoint=replace(p.cp, lease_expires_at=p.cp.lease_expires_at + timedelta(seconds=1)),
        )
    if state.startswith("foreign"):
        with pytest.raises(CheckpointConflict):
            await original.completed_claim_state(p.db, 1, claim, lock=p.lock)
    else:
        actual = await original.completed_claim_state(p.db, 1, claim, lock=p.lock)
        assert actual == (
            None if state.startswith("missing") else CheckpointState(p.run_id, 8, None)
        )
    assert all(
        c.args[0].is_select and (c.args[0]._for_update_arg is not None) == p.lock
        for c in p.db.execute.await_args_list
    )
    p.db.commit.assert_not_called()
    p.coordinator.assert_awaited_once_with(p.db, p.run_id, 1, lock=p.lock)
    p.active.assert_awaited_once_with(p.db, p.run_id, lock=p.lock)
