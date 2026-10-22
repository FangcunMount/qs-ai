"""Original first reference survives renewal; recovery runtime has no provider gateway."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_response import (
    CandidateResponseReference,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.workflow_transport import candidate_recovery_runtime as module
from qs_ai.infrastructure.workflow_transport.candidate_completion_sender import (
    CandidateCompletionPending,
)
from tests.test_evaluation_serial_response_receipts import original as original


def fixture(original, monkeypatch, *, response=True, renewed=False):
    p = original
    reference = CandidateResponseReference(p.ref.run_id, 1, p.ref.version, p.cp)
    claim = SlotClaim(p.ref.run_id, p.ref.version, p.cp)
    if renewed:
        claim = replace(
            claim,
            checkpoint=replace(
                p.cp, lease_expires_at=p.cp.lease_expires_at + timedelta(seconds=30)
            ),
        )
    source = SimpleNamespace(version=7, claim=claim, evidence=p.result if response else None)
    reader = AsyncMock(return_value=source)
    references = AsyncMock(return_value=SimpleNamespace(reference=reference, result=p.result))
    monkeypatch.setattr(module, "read_candidate_recovery_source", reader)
    monkeypatch.setattr(module, "read_candidate_response", references)
    active, opens = False, []
    db = SimpleNamespace(connection=AsyncMock(), commit=AsyncMock())

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        opens.append(True)
        try:
            yield db
        finally:
            active = False

    tx = SimpleNamespace(open=opened)
    runtime = module.CandidateRecoveryRuntime(
        tx, Mock(), Mock(), signing_key=None, recipient_key=None
    )
    assert not opens

    async def outside(*args):
        assert not active
        return True

    expiry, completion = AsyncMock(side_effect=outside), AsyncMock(side_effect=outside)
    runtime._expiry, runtime._completion = expiry, completion

    async def call():
        return await runtime(1, claim, claim.checkpoint.lease_expires_at)

    return SimpleNamespace(**locals())


@pytest.mark.parametrize("renewed", [False, True])
async def test_expired_candidate_response_keeps_original_dispatch_reference(
    original, monkeypatch, renewed
):
    f = fixture(original, monkeypatch, renewed=renewed)
    assert await f.call()
    f.completion.assert_awaited_once_with(f.reference)
    f.expiry.assert_not_called()
    f.reader.assert_awaited_once_with(
        f.db, 1, f.claim, f.claim.checkpoint.lease_expires_at, lock=False
    )
    assert len(f.opens) == 1
    f.db.commit.assert_not_called()


async def test_absent_first_response_uses_typed_original_expiry_without_fake_receipt(
    original, monkeypatch
):
    f = fixture(original, monkeypatch, response=False)
    assert await f.call()
    request, at = f.expiry.await_args.args
    assert request.claim == f.claim and at == f.claim.checkpoint.lease_expires_at
    f.completion.assert_not_called()
    f.references.assert_not_called()


async def test_unresolved_response_is_not_reclassified_or_retried(original, monkeypatch):
    f = fixture(original, monkeypatch)
    f.completion.side_effect = CandidateCompletionPending("Original result remains unresolved")
    with pytest.raises(CandidateCompletionPending):
        await f.call()
    f.expiry.assert_not_called()
    assert f.completion.await_count == 1


@pytest.mark.parametrize("state", ["completed", "conflict", "contradictory_response"])
async def test_invalid_or_finished_ownership_cannot_start_another_business_root(
    original, monkeypatch, state
):
    f = fixture(original, monkeypatch)
    if state == "completed":
        f.reader.return_value = None
        assert not await f.call()
    else:
        if state == "conflict":
            f.reader.side_effect = CheckpointConflict("Original claim changed")
        else:
            f.references.return_value = None
        with pytest.raises(CheckpointConflict):
            await f.call()
    f.expiry.assert_not_called()
    f.completion.assert_not_called()
    f.db.commit.assert_not_called()
