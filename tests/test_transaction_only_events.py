"""Actual EventSession root commit cannot silently use legacy state publication."""

import pytest
from reliable_messaging.transaction import BindingError

from qs_ai.infrastructure.persistence.mysql.database import EventSession, changed_evaluation
from qs_ai.infrastructure.workflow_transport.transaction_events import TransactionOnlyEvents


async def test_clean_operational_commit_finishes_its_original_root_without_a_publisher():
    async with EventSession() as db:
        guard = TransactionOnlyEvents()
        await db.begin()
        original = db.get_transaction()
        db.info["state_events"] = guard
        await db.commit()
        assert not original.is_active and not db.in_transaction()


async def test_evaluation_business_marker_rejects_root_commit_and_preserves_rollback():
    async with EventSession() as db:
        await db.begin()
        db.info["state_events"] = TransactionOnlyEvents()
        original = db.get_transaction()
        changed_evaluation(db, "owned:run")
        with pytest.raises(BindingError):
            await db.commit()
        assert original.is_active and db.get_transaction() is original
        await db.rollback()
        assert not original.is_active


async def test_interpretation_state_cannot_use_the_operational_commit_boundary():
    async with EventSession() as db:
        await db.begin()
        events = TransactionOnlyEvents()
        db.info["state_events"] = events
        with pytest.raises(BindingError):
            await events.record_interpretation(db, {})
        assert db.get_transaction().is_active


@pytest.mark.parametrize("fault", ["ended", "nested", "replaced"])
async def test_operational_boundary_requires_current_active_root_and_its_own_event_context(fault):
    async with EventSession() as db:
        await db.begin()
        events = TransactionOnlyEvents()
        db.info["state_events"] = events
        if fault == "ended":
            await db.rollback()
        elif fault == "nested":
            await db.begin_nested()
        else:
            db.info["state_events"] = object()
        with pytest.raises(BindingError):
            await events.flush(db)


async def test_shared_application_boundary_does_not_share_sessions_or_dirty_business_markers():
    events = TransactionOnlyEvents()
    async with EventSession() as left, EventSession() as right:
        for db in (left, right):
            await db.begin()
            db.info["state_events"] = events
        changed_evaluation(left, "owned:left")
        with pytest.raises(BindingError):
            await left.commit()
        await right.commit()
        assert left.in_transaction() and not right.in_transaction()
