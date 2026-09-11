from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint


@pytest.fixture
def checkpoint():
    at = datetime(2026, 9, 12, tzinfo=UTC)
    return ExecutionCheckpoint(
        "execution:1",
        "generation",
        "case:1",
        1,
        "",
        1,
        "worker:1",
        "invocation:1",
        "prepared",
        at,
        at + timedelta(seconds=30),
    )


def test_only_exact_expired_preparation_is_recoverable(checkpoint):
    expiry = checkpoint.lease_expires_at
    assert checkpoint.can_release_preparation("invocation:1", expiry, expiry)
    assert not checkpoint.can_release_preparation("invocation:old", expiry, expiry)
    assert not checkpoint.can_release_preparation(
        "invocation:1", expiry + timedelta(seconds=1), expiry
    )
    assert not checkpoint.can_release_preparation(
        "invocation:1", expiry, expiry - timedelta(seconds=1)
    )
    dispatched = checkpoint.mark_dispatching("worker:1", checkpoint.claimed_at)
    assert not dispatched.can_release_preparation(
        "invocation:1", expiry, expiry + timedelta(days=1)
    )
    assert checkpoint.phase == "prepared"


def test_owner_phase_and_original_qs_dispatch_time_bounds(checkpoint):
    with pytest.raises(ValueError):
        checkpoint.mark_dispatching("worker:other", checkpoint.claimed_at)
    for at in (
        checkpoint.claimed_at - timedelta(microseconds=1),
        checkpoint.lease_expires_at + timedelta(microseconds=1),
    ):
        with pytest.raises(ValueError):
            checkpoint.mark_dispatching("worker:1", at)
    # Retain QS's inclusive dispatch endpoint; recovery also starts at this endpoint.
    dispatched = checkpoint.mark_dispatching("worker:1", checkpoint.lease_expires_at)
    with pytest.raises(ValueError):
        dispatched.mark_dispatching("worker:1", checkpoint.lease_expires_at)


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "other"},
        {"candidate_id": "candidate:1"},
        {"kind": "semantic"},
        {"execution_ordinal": 3},
        {"slot_ordinal": 6},
        {"slot_ordinal": True},
        {"owner": "with space"},
        {"phase": "completed"},
        {"claimed_at": datetime(2026, 9, 12)},
    ],
)
def test_invalid_checkpoint_cannot_be_constructed(checkpoint, changes):
    with pytest.raises(ValueError):
        replace(checkpoint, **changes)
