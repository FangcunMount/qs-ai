"""Candidate scan cursor remains bounded; explicit sender owns its independent root."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_recovery as module
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_scan import RecoveryCursor
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from tests.test_evaluation_serial_response_receipts import original as original


@pytest.mark.parametrize("path", ["legacy", "transaction", "conflict", "pending", "unchanged"])
async def test_original_bounded_scan_closes_hint_before_dispatch_and_never_commits_new_root(
    original, monkeypatch, path
):
    p = original
    claim = SlotClaim(p.ref.run_id, p.ref.version, p.cp)
    row = dict(
        run_id=str(claim.run_id),
        organization_id=1,
        version=claim.version,
        case_id=p.cp.case_id,
        slot_ordinal=p.cp.slot_ordinal,
        checkpoint_json=encode(p.cp),
    )
    result = Mock()
    result.mappings.return_value.all.return_value = [row]
    db = SimpleNamespace(execute=AsyncMock(return_value=result), commit=AsyncMock())
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    tx = SimpleNamespace(open=opened)
    routes, schemas = object(), object()
    legacy = AsyncMock()
    monkeypatch.setattr(module, "recover_candidate", legacy)

    async def dispatch(organization_id, observed, at):
        assert not active and organization_id == 1 and observed == claim
        assert at == p.cp.lease_expires_at
        if path == "conflict":
            raise CheckpointConflict("Original source changed")
        if path == "pending":
            raise RuntimeError("Original responsibility unresolved")
        return path != "unchanged"

    cursor = RecoveryCursor()
    args = (tx, routes, schemas, p.cp.lease_expires_at, cursor)
    if path == "legacy":
        assert await module.recover_next_candidate(*args)
        legacy.assert_awaited_once_with(tx, 1, claim, p.cp.lease_expires_at, routes, schemas)
    elif path == "pending":
        with pytest.raises(RuntimeError):
            await module.recover_next_candidate(*args, dispatch=dispatch)
        legacy.assert_not_called()
    else:
        assert await module.recover_next_candidate(*args, dispatch=dispatch) is (
            path == "transaction"
        )
        legacy.assert_not_called()
    assert db.execute.await_args.args[0]._limit_clause.value == 64
    assert cursor.after_candidate == (
        (str(claim.run_id), p.cp.case_id, p.cp.slot_ordinal)
        if path in ("legacy", "transaction", "pending")
        else ("", "", 0)
    )
    db.commit.assert_not_called()
