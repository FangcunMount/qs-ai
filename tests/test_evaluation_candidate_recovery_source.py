"""Original candidate recovery owns expiry/fence checks and consumes first evidence once."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_completion as completion
from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_recovery as original
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim


@pytest.fixture
def source(monkeypatch):
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
    )
    claim = SlotClaim(run_id, 4, cp)
    ledger = Mock()
    ledger.first.return_value = None
    db = SimpleNamespace(execute=AsyncMock(return_value=ledger), commit=AsyncMock())

    @asynccontextmanager
    async def opened():
        yield db

    tx = SimpleNamespace(open=opened)
    completed = AsyncMock(return_value=None)
    monkeypatch.setattr(completion, "completed_claim_state", completed)
    locked = AsyncMock(return_value=({}, 7))
    active = AsyncMock(return_value=(claim,))
    required = AsyncMock(return_value=claim)
    release = AsyncMock()
    save = AsyncMock()
    response = AsyncMock(return_value=None)
    prepare = AsyncMock(return_value=object())
    finish = AsyncMock()
    for name, value in [
        ("lock_run", locked),
        ("active_claims", active),
        ("require_claim", required),
        ("release_claim", release),
        ("save_checkpoint", save),
        ("read_response", response),
        ("prepare_step", prepare),
        ("finish_step", finish),
    ]:
        monkeypatch.setattr(original, name, value)
    routes, schemas = Mock(), Mock()

    async def recover(recovery_at=cp.lease_expires_at, observed=claim):
        await original.recover_candidate(tx, 1, observed, recovery_at, routes, schemas)

    return SimpleNamespace(**locals())


async def test_original_expired_candidate_preparation_releases_same_claim_and_single_version(
    source,
):
    p = source
    await p.recover()
    p.release.assert_awaited_once_with(p.db, p.claim)
    p.save.assert_awaited_once_with(p.db, CheckpointState(p.run_id, 8, None), 7)
    p.db.commit.assert_awaited_once()
    p.response.assert_not_called()
    p.prepare.assert_not_called()
    p.finish.assert_not_called()


@pytest.mark.parametrize("fault", ["renewed", "not_expired", "prepared_has_dispatch"])
async def test_original_candidate_recovery_refuses_changed_expiry_or_prepared_dispatch(
    source, fault
):
    p = source
    if fault == "renewed":
        p.required.return_value = replace(
            p.claim,
            checkpoint=replace(p.cp, lease_expires_at=p.cp.lease_expires_at + timedelta(seconds=1)),
        )
    elif fault == "prepared_has_dispatch":
        p.ledger.first.return_value = object()
    with pytest.raises(CheckpointConflict):
        await p.recover(p.at if fault == "not_expired" else p.cp.lease_expires_at)
    p.release.assert_not_called()
    p.save.assert_not_called()
    p.db.commit.assert_not_called()
    p.prepare.assert_not_called()
    p.finish.assert_not_called()


async def test_completed_candidate_recovery_is_readonly_and_never_reopens_model_work(source):
    p = source
    p.completed.return_value = CheckpointState(p.run_id, 9, None)
    await p.recover()
    p.locked.assert_not_called()
    p.required.assert_not_called()
    p.db.commit.assert_not_called()
    p.prepare.assert_not_called()
    p.finish.assert_not_called()


@pytest.mark.parametrize("evidence", [False, True])
@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_candidate_recovery_keeps_response_or_unknown_without_model_retry(
    source, kind, evidence
):
    p = source
    cp = replace(
        p.cp, kind=kind, candidate_id="candidate:first" if kind == "semantic" else ""
    ).mark_dispatching(p.cp.owner, p.at)
    current = replace(p.claim, checkpoint=cp)
    p.required.return_value = current
    first = EvaluationResponse(object(), None, p.at + timedelta(seconds=1)) if evidence else None
    p.response.return_value = first
    await p.recover(observed=current)
    p.prepare.assert_awaited_once()
    assert p.prepare.await_args.kwargs["resume_claim"] == current
    assert p.prepare.await_args.kwargs["clock"]() == p.cp.lease_expires_at
    p.finish.assert_awaited_once()
    args, kwargs = p.finish.await_args.args, p.finish.await_args.kwargs
    assert args[7] is (first.response if first else None)
    assert kwargs["recovery_at"] == p.cp.lease_expires_at
    assert kwargs["clock"]() == (first.finished_at if first else p.cp.lease_expires_at)
    if not first:
        failure = args[8]
        assert failure.result_unknown and failure.code == "execution_interrupted"
        assert failure.evidence_refs == (cp.execution_id, cp.invocation_id)
    p.release.assert_not_called()
    p.save.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("evidence", [False, True])
@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_readonly_candidate_recovery_keeps_first_evidence_without_resuming_or_committing(
    source, kind, evidence
):
    p = source
    cp = replace(
        p.cp, kind=kind, candidate_id="candidate:first" if kind == "semantic" else ""
    ).mark_dispatching(p.cp.owner, p.at)
    current = replace(p.claim, checkpoint=cp)
    p.required.return_value = current
    result = EvaluationResponse(object(), None, p.at + timedelta(seconds=1)) if evidence else None
    p.response.return_value = result
    found = await original.read_candidate_recovery_source(
        p.db, 1, current, cp.lease_expires_at, lock=False
    )
    assert found.version == 7 and found.claim == current and found.evidence is result
    assert "execution:first" not in repr(found)
    p.completed.assert_awaited_once_with(p.db, 1, current, lock=False)
    p.locked.assert_awaited_once_with(p.db, p.run_id, 1, lock=False)
    p.required.assert_awaited_once_with(p.db, current, lock=False)
    p.prepare.assert_not_called()
    p.finish.assert_not_called()
    p.save.assert_not_called()
    p.release.assert_not_called()
    p.db.commit.assert_not_called()
