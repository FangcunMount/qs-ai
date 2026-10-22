"""Response and lease commits keep their own original session; business events are forbidden."""

import pytest
from reliable_messaging.transaction import BindingError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.infrastructure.persistence.mysql.database import changed_evaluation
from qs_ai.infrastructure.workflow_transport.candidate_runtime import _CandidateLocalEvents


async def test_candidate_operational_guard_accepts_only_clean_original_session():
    async with AsyncSession() as db:
        await db.begin()
        guard = _CandidateLocalEvents(db)
        db.info["state_events"] = guard
        await guard.flush(db)
        await guard.flush(db)  # Explicit validation then normal EventSession.commit validation.
        with pytest.raises(BindingError):
            await guard.record_interpretation(db, {})


@pytest.mark.parametrize(
    "fault", ["wrong_session", "root_ended", "nested", "recorder_replaced", "business_changed"]
)
async def test_candidate_operational_guard_cannot_suppress_a_business_transition_or_cross_roots(
    fault,
):
    async with AsyncSession() as db, AsyncSession() as other:
        await db.begin()
        guard = _CandidateLocalEvents(db)
        db.info["state_events"] = guard
        if fault == "root_ended":
            await db.rollback()
        elif fault == "nested":
            await db.begin_nested()
        elif fault == "recorder_replaced":
            db.info["state_events"] = object()
        elif fault == "business_changed":
            changed_evaluation(db, "run:changed")
        with pytest.raises(BindingError):
            await guard.flush(other if fault == "wrong_session" else db)


async def test_candidate_operational_guard_refuses_dirty_or_missing_root():
    async with AsyncSession() as db:
        with pytest.raises(BindingError):
            _CandidateLocalEvents(db)
        await db.begin()
        db.info["evaluation_events"] = {"run:pending"}
        with pytest.raises(BindingError):
            _CandidateLocalEvents(db)
