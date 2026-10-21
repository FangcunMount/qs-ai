"""Characterize the original prepared-checkpoint locks and externally supplied identities."""

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_preparation as preparation
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from tests.test_evaluation_resolution import AT


@pytest.fixture
def original(monkeypatch):
    run_id, policy = uuid4(), load_execution_policy()
    checkpoint = dict(version=7, checkpoint_json=None)
    creation = dict(
        audit=dict(created_at=(AT - timedelta(seconds=1)).isoformat()),
        execution_policy_json=policy.definition_json,
        slots=[],
        preflight=dict(status="passed", case_id="original:case"),
    )
    run = dict(
        progress_json=dict(status="collecting", preflight=creation["preflight"]),
        definition_json=json.dumps(creation),
        execution_mode="serial_v1",
    )
    order, query_kinds = [], []

    class DB:
        async def execute(self, statement):
            table = statement.get_final_froms()[0]
            order.append((table.name, statement._for_update_arg is not None))
            query_kinds.append(statement.compile().params.get("kind_1"))
            value = (
                checkpoint
                if table is preparation.evaluation_checkpoints
                else run
                if table is preparation.evaluation_runs
                else None
            )
            return SimpleNamespace(
                mappings=lambda: SimpleNamespace(one_or_none=lambda: value, all=lambda: [])
            )

    db = DB()
    frozen = Mock(return_value=(policy, None))
    project = Mock(return_value=())
    action = SimpleNamespace(
        kind="generation",
        resume=False,
        case_id="original:case",
        slot_ordinal=1,
        candidate_id="",
        execution_ordinal=1,
    )
    choose = Mock(return_value=action)
    monkeypatch.setattr(preparation, "frozen_policies", frozen)
    monkeypatch.setattr(preparation, "project_slots", project)
    monkeypatch.setattr(preparation, "next_action", choose)

    async def save(_db, state, version):
        assert _db is db and version == 7 and state.run_id == run_id
        order.append(("checkpoint_cas", state.version))

    save_checkpoint = AsyncMock(side_effect=save)
    monkeypatch.setattr(preparation, "save_checkpoint", save_checkpoint)
    return SimpleNamespace(**locals())


async def call(p, **changes):
    args = dict(
        db=p.db,
        run_id=p.run_id,
        expected_version=7,
        organization_id=1,
        owner="worker:original",
        execution_id="execution:first",
        invocation_id="invocation:first",
        at=AT,
        lease_expires_at=AT + timedelta(minutes=5),
    )
    args.update(changes)
    return await preparation.prepare_execution(**args)


async def test_original_preparation_keeps_five_lock_reads_before_one_checkpoint_cas(original):
    p = original
    state = await call(p)
    assert p.order == [
        (preparation.evaluation_checkpoints.name, True),
        (preparation.evaluation_runs.name, True),
        (preparation.evaluation_dispatches.name, True),
        (preparation.evaluation_generation_completions.c.kind.table.name, True),
        (preparation.evaluation_semantic_completions.c.kind.table.name, True),
        ("checkpoint_cas", 8),
    ]
    assert p.query_kinds == [None, None, None, "generation", "semantic"]
    assert state.version == 8 and state.checkpoint.phase == "prepared"
    assert (state.checkpoint.execution_id, state.checkpoint.invocation_id) == (
        "execution:first",
        "invocation:first",
    )
    assert state.checkpoint.claimed_at == AT
    assert state.checkpoint.lease_expires_at == AT + timedelta(minutes=5)


@pytest.mark.parametrize("fault", ["version", "active", "candidate_mode", "cancel", "policy"])
async def test_original_guard_rejection_never_advances_checkpoint(original, fault):
    p = original
    if fault == "version":
        p.checkpoint["version"] += 1
    elif fault == "active":
        p.checkpoint["checkpoint_json"] = {}
    elif fault == "candidate_mode":
        p.run["execution_mode"] = "candidate_v2"
    elif fault == "cancel":
        p.run["progress_json"]["cancel_requested"] = {"original": True}
    else:
        p.run["definition_json"] = json.dumps({**p.creation, "execution_policy_json": "changed"})
    with pytest.raises(CheckpointConflict):
        await call(p)
    p.save_checkpoint.assert_not_called()


@pytest.mark.parametrize("fault", ["early_creation", "early_recovery", "terminal", "resume"])
async def test_original_clock_and_action_rules_precede_checkpoint_write(original, fault):
    p = original
    changes = {}
    if fault == "early_creation":
        changes["at"] = AT - timedelta(seconds=2)
    elif fault == "early_recovery":
        p.run["progress_json"]["semantic_contract_recoveries"] = [
            {"resolved_at": (AT + timedelta(seconds=1)).isoformat()}
        ]
    elif fault == "terminal":
        p.action.kind = "terminal"
    else:
        p.action.resume = True
    with pytest.raises((ValueError, CheckpointConflict)):
        await call(p, **changes)
    p.save_checkpoint.assert_not_called()


async def test_readonly_preparation_keeps_kind_scopes_and_frozen_identifiers_without_locks_or_cas(
    original,
):
    p = original
    plan = await preparation.plan_prepare_execution(
        p.db,
        p.run_id,
        7,
        1,
        "worker:original",
        "execution:first",
        "invocation:first",
        AT,
        AT + timedelta(minutes=5),
    )
    assert len(p.order) == 5 and all(locked is False for _, locked in p.order)
    assert p.query_kinds == [None, None, None, "generation", "semantic"]
    p.save_checkpoint.assert_not_called()
    assert plan.checkpoint.phase == "prepared" and plan.version == 8
    assert await call(p) == plan
