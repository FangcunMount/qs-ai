"""Original decision audit and lock/write order before extracting read-only planning."""

import json
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.sql import Select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.resolution import UnknownExecution
from qs_ai.infrastructure.persistence.mysql import evaluation_resolution as resolution
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from tests.test_evaluation_resolution import AT, decision


@pytest.fixture
def original(monkeypatch):
    run_id = uuid4()
    checkpoint = dict(version=7, checkpoint_json=None)
    progress = dict(
        status="blocked",
        transitions=[{"from": "collecting", "to": "blocked", "evidence_refs": ["original"]}],
        unresolved_result_unknown_count=1,
        retained_field={"original": [1, 2]},
    )
    run = dict(
        organization_id=1,
        progress_json=progress,
        definition_json=json.dumps(
            dict(audit=dict(created_at=(AT - timedelta(hours=1)).isoformat()))
        ),
        execution_mode="serial_v1",
    )
    order, written = [], []

    class DB:
        async def execute(self, statement):
            if isinstance(statement, Select):
                table = statement.get_final_froms()[0].name
                order.append((table, statement._for_update_arg is not None))
                value = checkpoint if table == "evaluation_checkpoints" else run
                return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: value))
            order.append(("write_run",))
            written.append(statement.compile().params["progress_json"])

    db = DB()
    evidence = SimpleNamespace(
        unknowns=(UnknownExecution("execution:1", "generation", 1, AT - timedelta(seconds=1)),),
        resolutions=(),
        policy=load_execution_policy(),
    )
    load = AsyncMock(return_value=evidence)
    monkeypatch.setattr(resolution, "load_resolution_evidence", load)

    async def save(_db, state, expected):
        assert _db is db and state.run_id == run_id and expected == 7
        order.append(("checkpoint_cas", state.version))

    save_checkpoint = AsyncMock(side_effect=save)
    monkeypatch.setattr(resolution, "save_checkpoint", save_checkpoint)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["authorize_replacement", "cancel_run"])
async def test_original_resolution_locks_checkpoint_then_run_before_cas_and_preserves_audit(
    original, kind
):
    p = original
    value = decision(decision=kind)
    before = json.dumps(p.progress)
    state = await resolution.accept_resolution(p.db, p.run_id, 7, 1, value, confirm=True)
    assert (state.version, state.checkpoint) == (8, None)
    assert p.order == [
        ("evaluation_checkpoints", True),
        ("evaluation_runs", True),
        ("checkpoint_cas", 8),
        ("write_run",),
    ]
    assert json.dumps(p.progress) == before
    updated = p.written[0]
    assert updated["retained_field"] == p.progress["retained_field"]
    assert updated["result_unknown_resolutions"][0]["resolved_at"] == value.resolved_at.isoformat()
    assert updated["transitions"][0] == p.progress["transitions"][0]
    assert updated["transitions"][-1]["evidence_refs"] == [value.execution_id]
    assert updated["status"] == ("canceled" if kind == "cancel_run" else "collecting")


async def test_original_confirmation_precedes_any_io(original):
    p = original
    with pytest.raises(ValueError, match="confirmation"):
        await resolution.accept_resolution(p.db, p.run_id, 7, 1, decision(), confirm=False)
    assert not p.order
    p.load.assert_not_called()


@pytest.mark.parametrize("fault", ["checkpoint", "active", "missing_run", "early"])
async def test_original_invalid_resolution_never_writes(original, fault):
    p = original
    value = decision()
    if fault == "checkpoint":
        p.checkpoint["version"] = 8
    elif fault == "active":
        p.checkpoint["checkpoint_json"] = {}
    elif fault == "missing_run":
        p.run["progress_json"] = None
    else:
        value = replace(value, resolved_at=AT - timedelta(hours=2))
    with pytest.raises((CheckpointConflict, ValueError)):
        await resolution.accept_resolution(p.db, p.run_id, 7, 1, value, confirm=True)
    assert not p.written
    p.save_checkpoint.assert_not_called()


@pytest.mark.parametrize("kind", ["authorize_replacement", "cancel_run"])
async def test_readonly_plan_has_no_locks_or_writes_and_matches_original_audit(original, kind):
    p = original
    value = decision(decision=kind)
    before = json.dumps(p.progress)
    planned = await resolution.plan_resolution(p.db, p.run_id, 7, 1, value, confirm=True)
    assert p.order == [("evaluation_checkpoints", False), ("evaluation_runs", False)]
    assert planned.state.version == 8 and planned.state.checkpoint is None
    assert json.dumps(p.progress) == before
    assert not p.written
    p.save_checkpoint.assert_not_called()
    changed = planned.progress()
    changed["retained_field"]["original"].append(3)
    assert planned.progress()["retained_field"] == {"original": [1, 2]}
    assert "retained_field" not in repr(planned)
    await resolution.accept_resolution(p.db, p.run_id, 7, 1, value, confirm=True)
    assert p.written[0] == planned.progress()


async def test_readonly_plan_revalidates_candidate_claim_guard_before_evidence(
    original, monkeypatch
):
    from qs_ai.infrastructure.persistence.mysql import evaluation_slot_claims

    p = original
    p.run["execution_mode"] = "candidate_v2"
    claims = AsyncMock(return_value=True)
    monkeypatch.setattr(evaluation_slot_claims, "active_claims", claims)
    with pytest.raises(CheckpointConflict, match="Drain"):
        await resolution.plan_resolution(p.db, p.run_id, 7, 1, decision(), confirm=True)
    claims.assert_awaited_once_with(p.db, p.run_id)
    p.load.assert_not_called()
    p.save_checkpoint.assert_not_called()
    assert not p.written
