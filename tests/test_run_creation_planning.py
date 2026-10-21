"""Original creation bytes and root ownership before extracting read-only planning."""

import hashlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from qs_ai.infrastructure.persistence.mysql import evaluation_runs as runs
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import EvaluationContracts
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import load_suite
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release


@pytest.fixture
async def context(monkeypatch, complete_release):
    release = complete_release
    suite = load_suite(release.suite)
    contracts = EvaluationContracts(
        load_execution_policy(), load_gate_policy(), load_semantic_assets()
    )
    read_suite = AsyncMock(return_value=suite)
    read_contracts = AsyncMock(return_value=contracts)
    freeze = AsyncMock()
    monkeypatch.setattr(runs, "load_registered_suite", read_suite)
    monkeypatch.setattr(runs, "evaluation_contracts", read_contracts)
    monkeypatch.setattr(runs, "freeze_policy", freeze)
    db = SimpleNamespace(
        execute=AsyncMock(),
        commit=AsyncMock(),
        rollback=AsyncMock(),
        info={"state_events": object()},
    )
    run_id = UUID("11111111-2222-4333-8444-555555555555")
    at = datetime(2026, 10, 21, 8, 1, 2, tzinfo=timezone(timedelta(hours=8)))
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
async def test_original_creation_freezes_definition_and_policy_in_caller_root(context, mode):
    f = context
    state = await runs.create_run(
        f.db, f.run_id, f.release, 1, "user:42", "原创建意图", f.at, execution_mode=mode
    )
    assert state.run_id == f.run_id and state.version == 1 and state.checkpoint is None
    statements = [c.args[0] for c in f.db.execute.await_args_list]
    assert [s.table.name for s in statements] == ["evaluation_runs", "evaluation_checkpoints"]
    values = statements[0].compile().params
    assert values["run_id"] == str(f.run_id) and values["execution_mode"] == mode
    raw = values["definition_json"]
    assert hashlib.sha256(raw.encode()).hexdigest() == (
        "93d94137c095ce997761763c5e180d127bc22145060dde86ab71d5aa1a277708"
    )
    assert f.at.isoformat() in raw
    assert f.db.info["evaluation_events"] == {str(f.run_id)}
    f.freeze.assert_awaited_once_with(f.db, f.run_id, f.contracts.execution)
    f.db.commit.assert_not_called()
    f.db.rollback.assert_not_called()


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
async def test_readonly_creation_plan_preserves_exact_bytes_without_writes_or_dirty_event(
    context, mode
):
    f = context
    plan = await runs.plan_run_creation(
        f.db, f.run_id, f.release, 1, "user:42", "原创建意图", f.at, execution_mode=mode
    )
    assert plan.run_id == f.run_id and plan.organization_id == 1 and plan.requested_by == "user:42"
    assert plan.execution_mode == mode and plan.policy is f.contracts.execution
    assert hashlib.sha256(plan.definition_json.encode()).hexdigest() == (
        "93d94137c095ce997761763c5e180d127bc22145060dde86ab71d5aa1a277708"
    )
    assert "原创建意图" not in repr(plan)
    f.db.execute.assert_not_called()
    f.freeze.assert_not_called()
    assert "evaluation_events" not in f.db.info
    from dataclasses import FrozenInstanceError

    with pytest.raises(FrozenInstanceError):
        plan.execution_mode = "changed"
    state = await runs.apply_run_creation(f.db, plan)
    assert state.version == 1 and state.run_id == f.run_id
    assert f.db.info["evaluation_events"] == {str(f.run_id)}
    f.db.commit.assert_not_called()
    f.db.rollback.assert_not_called()


@pytest.mark.parametrize("invalid", ["mode", "identity", "reason", "clock"])
async def test_invalid_creation_is_rejected_before_any_registry_or_business_io(context, invalid):
    f = context
    run_id, reason, at, mode = f.run_id, "原创建意图", f.at, "serial_v1"
    if invalid == "mode":
        mode = "invalid"
    elif invalid == "identity":
        run_id = UUID(int=0)
    elif invalid == "reason":
        reason = "x" * 1001
    else:
        at = at.replace(tzinfo=None)
    with pytest.raises(ValueError):
        await runs.plan_run_creation(
            f.db, run_id, f.release, 1, "user:42", reason, at, execution_mode=mode
        )
    f.read_suite.assert_not_called()
    f.read_contracts.assert_not_called()
    f.db.execute.assert_not_called()
    f.freeze.assert_not_called()


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
async def test_pure_creation_freeze_uses_original_suite_contract_bytes_without_registry_io(
    context, mode
):
    f = context
    plan = runs.freeze_run_creation(
        f.run_id,
        f.release,
        1,
        "user:42",
        "原创建意图",
        f.at,
        suite=f.suite,
        contracts=f.contracts,
        execution_mode=mode,
    )
    assert hashlib.sha256(plan.definition_json.encode()).hexdigest() == (
        "93d94137c095ce997761763c5e180d127bc22145060dde86ab71d5aa1a277708"
    )
    assert plan.execution_mode == mode and plan.policy is f.contracts.execution
    f.read_suite.assert_not_called()
    f.read_contracts.assert_not_called()
    f.db.execute.assert_not_called()
    f.db.commit.assert_not_called()


async def test_pure_freeze_refuses_suite_substitution_without_registry_reads(context):
    from dataclasses import replace

    f = context
    changed = replace(
        f.suite, reference=replace(f.suite.reference, fingerprint="sha256:" + "f" * 64)
    )
    with pytest.raises(ValueError):
        runs.freeze_run_creation(
            f.run_id,
            f.release,
            1,
            "user:42",
            "原创建意图",
            f.at,
            suite=changed,
            contracts=f.contracts,
        )
    f.read_suite.assert_not_called()
    f.read_contracts.assert_not_called()
    f.db.execute.assert_not_called()
