"""Capacity planning reads original quota math without reserving or taking root locks."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.domain.interpretation.model import Actor, RuleViolation, Session
from qs_ai.infrastructure.persistence.mysql import participant_capacity as capacity


def fixture(monkeypatch, rows=(), prior=None, policy=None):
    session = Session(str(uuid4()), Actor("7", "9"), "19", ("42",), "goal")
    session.queue(str(uuid4()))
    first, second = Mock(), Mock()
    first.mappings.return_value.one_or_none.return_value = prior
    second.mappings.return_value.all.return_value = list(rows)
    db = SimpleNamespace(execute=AsyncMock(side_effect=[first, second]))
    policy = policy or ParticipantCapacityPolicy()
    read_policy = AsyncMock(return_value=(policy, {"revision": "original"}))
    monkeypatch.setattr(capacity, "participant_policy", read_policy)
    return session, db, read_policy


async def test_capacity_preview_only_reads_without_locking_reserving_or_committing(monkeypatch):
    session, db, policy = fixture(monkeypatch)
    await capacity.preview(
        db, session, ParticipantCapacityPolicy(), datetime(2026, 10, 21, tzinfo=UTC)
    )
    assert db.execute.await_count == 2
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in db.execute.await_args_list
    )
    policy.assert_awaited_once()


@pytest.mark.parametrize("limit", ["org", "user", "assessment"])
async def test_preview_keeps_original_daily_capacity_refusal(monkeypatch, limit):
    limits = {"daily_org": 10, "daily_user": 10, "daily_assessment": 10}
    limits["daily_" + limit] = 1
    session, db, _ = fixture(
        monkeypatch,
        [{"subject_id": "9", "assessment_ids": ["42"]}],
        policy=ParticipantCapacityPolicy(**limits),
    )
    with pytest.raises(RuleViolation, match="participant_daily_capacity_exceeded"):
        await capacity.preview(db, session, ParticipantCapacityPolicy(), datetime.now(UTC))
    assert all(c.args[0].is_select for c in db.execute.await_args_list)


async def test_preview_preserves_exact_prior_reservation_without_reading_new_policy(monkeypatch):
    session, db, policy = fixture(monkeypatch)
    row = {
        "session_id": session.id,
        "organization_id": 7,
        "subject_id": "9",
        "assessment_ids": ["42"],
    }
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    db.execute.side_effect = [result]
    await capacity.preview(db, session, ParticipantCapacityPolicy(), datetime.now(UTC))
    assert db.execute.await_count == 1
    policy.assert_not_called()


async def test_preview_rejects_another_scope_prior_reservation(monkeypatch):
    session, db, policy = fixture(
        monkeypatch,
        prior={
            "session_id": "foreign",
            "organization_id": 8,
            "subject_id": "9",
            "assessment_ids": ["42"],
        },
    )
    with pytest.raises(RuleViolation, match="participant_reservation_conflict"):
        await capacity.preview(db, session, ParticipantCapacityPolicy(), datetime.now(UTC))
    policy.assert_not_called()


async def test_existing_authoritative_prior_read_keeps_its_original_lock(monkeypatch):
    session, db, _ = fixture(monkeypatch)
    assert await capacity.prior(db, session) is None
    assert db.execute.await_args.args[0]._for_update_arg is not None
