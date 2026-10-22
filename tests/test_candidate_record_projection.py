"""Characterize original record ordering and active-slot budget before readonly projection."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.infrastructure.persistence.mysql import evaluation_candidate_plan as original
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import SlotClaim
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from tests.test_evaluation_parallel_plan import plan as slots
from tests.test_semantic_readonly_source import source


@pytest.mark.parametrize("boundary", ["locked", "readonly", "records"])
@pytest.mark.parametrize("limit", [1, 3])
@pytest.mark.parametrize("active_slot", [False, True])
@pytest.mark.parametrize("unknown", [0, 1])
async def test_original_candidate_record_order_active_budget_and_unknown_guard(
    monkeypatch, limit, active_slot, unknown, boundary
):
    run_id, policy = uuid4(), load_execution_policy()
    cp = replace(source().cp, case_id="case:0", phase="prepared", dispatch_started_at=None)
    active = (SlotClaim(run_id, 3, cp),) if active_slot else ()
    creation = dict(slots=[], preflight=dict(status="passed", case_id="preflight:1"))
    progress = dict(status="collecting", unresolved_result_unknown_count=unknown)
    run = dict(definition_json=json.dumps(creation), progress_json=progress)
    dispatches = [dict(invocation_id="original:completed")]
    generations = [dict(invocation_id="original:completed", execution_id="generation:first")]
    semantics = []
    results = []
    for value in (dispatches, generations, semantics):
        result = Mock()
        result.mappings.return_value.all.return_value = value
        results.append(result)
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=results), commit=AsyncMock(), rollback=AsyncMock()
    )
    frozen = Mock(return_value=(policy, None))
    projected = Mock(return_value=slots())
    monkeypatch.setattr(original, "frozen_policies", frozen)
    monkeypatch.setattr(original, "project_slots", projected)
    if boundary == "records":
        actual = await original.project_candidate_records(
            creation,
            progress,
            policy,
            active,
            limit,
            dispatches=dispatches,
            generations=generations,
            semantics=semantics,
        )
    else:
        actual = await original.candidate_plan(
            db, run_id, run, active, limit, lock=boundary == "locked"
        )
    assert len(actual.ready) == (0 if unknown else max(0, limit - len(active)))
    if unknown:
        assert actual.control.cause == "result_unknown_requires_review"
    elif active_slot and actual.ready:
        assert actual.ready[0].slot_ordinal == 2
    if boundary == "records":
        frozen.assert_not_called()
    else:
        frozen.assert_called_once_with(creation)
    args = projected.call_args
    assert args.args == (creation["slots"], generations, dispatches, semantics, [], [])
    assert args.kwargs == dict(policy=policy)
    calls = db.execute.await_args_list
    assert len(calls) == (0 if boundary == "records" else 3)
    if calls:
        assert all(c.args[0].is_select for c in calls)
        assert all((c.args[0]._for_update_arg is not None) == (boundary == "locked") for c in calls)
        assert all(c.args[0].compile().params["run_id_1"] == str(run_id) for c in calls)
        assert calls[1].args[0].compile().params["kind_1"] == "generation"
        assert calls[2].args[0].compile().params["kind_1"] == "semantic"
    assert progress == dict(status="collecting", unresolved_result_unknown_count=unknown)
    db.commit.assert_not_called()
    db.rollback.assert_not_called()
