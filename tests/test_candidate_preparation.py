"""Protect the original candidate selection and coordinator CAS before sharing its preview."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_plan as module
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim


@pytest.fixture
def original(monkeypatch):
    run_id, at, db = uuid4(), datetime(2026, 10, 22, tzinfo=UTC), SimpleNamespace()
    action = SimpleNamespace(
        kind="generation", case_id="case:1", slot_ordinal=2, candidate_id="", execution_ordinal=1
    )
    run = dict(
        definition_json=json.dumps(
            dict(audit=dict(created_at=(at - timedelta(seconds=1)).isoformat()))
        ),
        progress_json=dict(status="collecting"),
    )
    observed = AsyncMock(return_value=(run, 7))
    active = AsyncMock(return_value=())
    plan = AsyncMock(return_value=SimpleNamespace(ready=(action,)))

    async def claim(db, run_id, cp, version):
        return SlotClaim(run_id, version, cp)

    created, saved = AsyncMock(side_effect=claim), AsyncMock()
    for name, value in (
        ("lock_run", observed),
        ("active_claims", active),
        ("candidate_plan", plan),
        ("create_claim", created),
        ("save_checkpoint", saved),
    ):
        monkeypatch.setattr(module, name, value)

    async def prepare():
        return await module.prepare_candidate(
            db,
            run_id,
            1,
            "worker:first",
            "execution:first",
            "invocation:first",
            at,
            at + timedelta(minutes=5),
            limit=3,
        )

    return SimpleNamespace(**locals())


@pytest.mark.parametrize("semantic", [False, True])
async def test_original_candidate_claim_keeps_next_action_first_identity_and_single_cas(
    original, semantic
):
    p = original
    if semantic:
        p.action.kind, p.action.candidate_id = "semantic", "candidate:first"
    claim = await p.prepare()
    assert claim.version == 8 and claim.run_id == p.run_id
    cp = claim.checkpoint
    assert (cp.execution_id, cp.invocation_id, cp.owner) == (
        "execution:first",
        "invocation:first",
        "worker:first",
    )
    assert (cp.kind, cp.case_id, cp.slot_ordinal, cp.candidate_id) == (
        p.action.kind,
        "case:1",
        2,
        p.action.candidate_id,
    )
    assert cp.phase == "prepared" and cp.claimed_at == p.at
    p.observed.assert_awaited_once_with(p.db, p.run_id, 1)
    p.active.assert_awaited_once_with(p.db, p.run_id)
    p.plan.assert_awaited_once_with(p.db, p.run_id, p.run, (), 3)
    p.saved.assert_awaited_once_with(p.db, CheckpointState(p.run_id, 8, None), 7)


@pytest.mark.parametrize(
    "damage", ["cancel", "missing", "capacity", "before_creation", "before_recovery"]
)
async def test_original_candidate_refuses_invalid_source_before_claim_or_coordinator_write(
    original, damage
):
    p = original
    error = CheckpointConflict
    if damage == "cancel":
        p.run["progress_json"]["cancel_requested"] = {"version": 7}
    elif damage == "missing":
        p.run["progress_json"] = None
    elif damage == "capacity":
        p.plan.return_value.ready = ()
    elif damage == "before_creation":
        p.run["definition_json"] = json.dumps(
            dict(audit=dict(created_at=(p.at + timedelta(seconds=1)).isoformat()))
        )
        error = ValueError
    else:
        p.run["progress_json"]["semantic_contract_recoveries"] = [
            dict(resolved_at=(p.at + timedelta(seconds=1)).isoformat())
        ]
        error = ValueError
    with pytest.raises(error):
        await p.prepare()
    p.created.assert_not_called()
    p.saved.assert_not_called()
