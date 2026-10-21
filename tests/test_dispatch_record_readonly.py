"""Original immutable dispatch budget, stage scope and insert ordering before extraction."""

import json
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.sql.dml import Insert

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql import evaluation_dispatches as dispatch
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from tests.test_evaluation_resolution import AT


@pytest.fixture
def original():
    run_id, policy = uuid4(), load_execution_policy()
    cp = ExecutionCheckpoint(
        "execution:first",
        "generation",
        "case:1",
        1,
        "",
        1,
        "worker:original",
        "invocation:first",
        "dispatching",
        AT,
        AT + timedelta(minutes=5),
        AT,
    )
    entries, order, inserted, scopes = [], [], [], []

    class DB:
        async def execute(self, statement):
            if isinstance(statement, Insert):
                order.append("insert")
                inserted.append(statement.compile().params)
                return None
            table = statement.get_final_froms()[0]
            if table is dispatch.evaluation_runs:
                order.append("frozen_policy")
                return SimpleNamespace(scalar_one=lambda: policy.definition_json)
            assert table is dispatch.dispatches
            order.append(("dispatch_budget", statement._for_update_arg is not None))
            scopes.append(statement.compile().params["kind_1"])
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: entries))

    db = DB()
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_original_stage_budget_is_locked_before_exact_immutable_dispatch_insert(
    original, kind
):
    p = original
    cp = p.cp if kind == "generation" else replace(p.cp, kind=kind, candidate_id="candidate:1")
    await dispatch.record_dispatch(p.db, p.run_id, cp)
    assert p.order == ["frozen_policy", ("dispatch_budget", True), "insert"]
    assert p.scopes == [kind] and len(p.inserted) == 1
    values = p.inserted[0]
    assert (values["run_id"], values["execution_id"], values["invocation_id"]) == (
        str(p.run_id),
        cp.execution_id,
        cp.invocation_id,
    )
    assert values["kind"] == kind and values["candidate_id"] == cp.candidate_id
    assert values["checkpoint_json"] == dispatch.encode(cp)


@pytest.mark.parametrize("fault", ["run_budget", "target_budget", "ordinal"])
async def test_original_exhausted_or_non_next_dispatch_never_inserts(original, fault):
    p = original
    if fault == "run_budget":
        p.entries.extend(
            dict(case_id="other", slot_ordinal=1) for _ in range(p.policy.generation_per_run)
        )
    else:
        p.entries.extend(
            dict(case_id=p.cp.case_id, slot_ordinal=p.cp.slot_ordinal)
            for _ in range(p.policy.generation_per_slot if fault == "target_budget" else 1)
        )
    with pytest.raises(CheckpointConflict):
        await dispatch.record_dispatch(p.db, p.run_id, p.cp)
    assert not p.inserted


async def test_original_semantic_budget_counts_exact_candidate_without_adopting_another(original):
    p = original
    p.entries.append(
        dict(case_id=p.cp.case_id, slot_ordinal=p.cp.slot_ordinal, candidate_id="other:1")
    )
    cp = replace(p.cp, kind="semantic", candidate_id="candidate:1")
    await dispatch.record_dispatch(p.db, p.run_id, cp)
    assert p.scopes == ["semantic"] and p.inserted[0]["candidate_id"] == cp.candidate_id
    assert (
        json.loads(p.policy.definition_json)["semantic_budget"]["max_executions_per_candidate"] == 2
    )


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_readonly_budget_plan_has_no_locks_or_insert_and_preserves_exact_record(
    original, kind
):
    p = original
    cp = p.cp if kind == "generation" else replace(p.cp, kind=kind, candidate_id="candidate:1")
    plan = await dispatch.plan_dispatch_record(p.db, p.run_id, cp)
    assert p.order == ["frozen_policy", ("dispatch_budget", False)]
    assert p.scopes == [kind] and not p.inserted
    changed = plan.values()
    changed["checkpoint_json"]["phase"] = "changed"
    assert plan.values()["checkpoint_json"] == dispatch.encode(cp)
    assert "checkpoint_json" not in repr(plan)
    await dispatch.record_dispatch(p.db, p.run_id, cp)
    assert p.inserted == [plan.values()]
