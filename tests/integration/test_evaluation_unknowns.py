"""Real disposable MySQL; synthetic provider calls do not establish business acceptance."""

import json
from dataclasses import asdict, replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql import evaluation_unknowns
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_generation_completions,
    evaluation_runs,
    evaluation_semantic_completions,
)
from qs_ai.infrastructure.workflows.evaluation import execute_step
from tests.integration.test_evaluation_recovery import EXPIRY, pending, recover
from tests.integration.test_evaluation_resolution import decision
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


async def snapshot(tx, run_id):
    async with tx.open() as db:
        records = [
            list((await db.execute(select(t).where(t.c.run_id == str(run_id)))).mappings())
            for t in (
                evaluation_dispatches,
                evaluation_generation_completions,
                evaluation_semantic_completions,
            )
        ]
    return await rows(tx, run_id), records


@pytest.mark.parametrize("semantic", [False, True])
async def test_read_original_unknown_target_receipt_and_budget_without_any_write(ready, semantic):
    tx, run_id, *_ = ready
    version = (await step(ready, Gateway(ready))).version if semantic else 3
    state = await recover(ready, await pending(ready, dispatched=True, version=version))
    store, scope = MySQLEvaluationManagement(tx), ManagementScope(run_id, 1, 42)
    before = await snapshot(tx, run_id)
    view = await store.list_unknowns(scope, state.version)
    assert view.run_id == str(run_id) and view.version == state.version
    assert view.status == "blocked" and view.can_resolve
    assert view.unresolved_result_unknown_count == len(view.executions) == 1
    record = view.executions[0]
    assert record.execution_id == "execution:dead" and record.invocation_id
    assert record.kind == ("semantic" if semantic else "generation")
    assert bool(record.candidate_id) is semantic
    assert record.execution_ordinal == record.target_execution_count == 1
    assert record.target_execution_limit == 2 and record.stage_execution_limit == 70
    assert record.stage_execution_count == 1 and record.replacement_allowed
    assert record.provider_call_count == 1 and record.started_at <= record.finished_at
    assert record.failure_stage == ("semantic_evaluation" if semantic else "generation_execution")
    assert record.failure_code
    # No raw provider body, prompt, model output or credential in this bounded metadata view.
    raw = json.dumps(asdict(view))
    assert all(
        key not in raw for key in ("raw_output", "normalized_output", "system_message", "api_key")
    )
    assert await store.list_unknowns(scope, state.version) == view
    assert await snapshot(tx, run_id) == before
    await store.resolve(scope, state.version, replace(decision(), actor=scope.actor), confirm=True)
    current = await store.list_unknowns(scope, state.version + 1)
    assert current.executions == () and current.unresolved_result_unknown_count == 0
    assert not current.can_resolve
    assert (await snapshot(tx, run_id))[1] == before[1]


async def test_scope_version_and_active_execution_do_not_expose_unknowns(ready):
    tx, run_id, *_ = ready
    store, scope = MySQLEvaluationManagement(tx), ManagementScope(run_id, 1, 42)
    active = await pending(ready, dispatched=True)
    with pytest.raises(CheckpointConflict, match="active"):
        await store.list_unknowns(scope, active.version)
    state = await recover(ready, active)
    before = await snapshot(tx, run_id)
    for foreign in (replace(scope, organization_id=2), replace(scope, run_id=uuid4())):
        with pytest.raises(NotFound):
            await store.list_unknowns(foreign, state.version)
    with pytest.raises(CheckpointConflict, match="version"):
        await store.list_unknowns(scope, state.version - 1)
    for invalid in (0, -1, True, 2**63):
        with pytest.raises(ValueError):
            await store.list_unknowns(scope, invalid)
    assert await snapshot(tx, run_id) == before


async def test_second_unknown_is_visible_but_replacement_is_not_offered_past_budget(ready):
    tx, run_id, _, routes, schemas = ready
    store, scope = MySQLEvaluationManagement(tx), ManagementScope(run_id, 1, 42)
    state = await recover(ready, await pending(ready, dispatched=True))
    await store.resolve(scope, state.version, replace(decision(), actor=scope.actor), confirm=True)
    state = await execute_step(
        tx,
        run_id,
        state.version + 1,
        1,
        "worker:replacement",
        Gateway(ready, fail=True),
        routes,
        schemas,
        clock=lambda: EXPIRY + timedelta(seconds=1),
    )
    view = await store.list_unknowns(scope, state.version)
    assert view.can_resolve and len(view.executions) == 1
    record = view.executions[0]
    assert record.execution_id != "execution:dead"
    assert (
        record.execution_ordinal
        == record.target_execution_count
        == record.target_execution_limit
        == 2
    )
    assert not record.replacement_allowed
    value = replace(
        decision(),
        actor=scope.actor,
        execution_id=record.execution_id,
        decision="cancel_run",
        resolved_at=EXPIRY + timedelta(seconds=2),
    )
    updated = await store.resolve(scope, state.version, value, confirm=True)
    canceled = await store.list_unknowns(scope, updated.version)
    assert canceled.status == "canceled" and not canceled.can_resolve
    assert canceled.executions == ()


async def test_resolution_during_read_does_not_mix_versions_or_enable_stale_write(
    ready, monkeypatch
):
    tx, run_id, *_ = ready
    store, scope = MySQLEvaluationManagement(tx), ManagementScope(run_id, 1, 42)
    state = await recover(ready, await pending(ready, dispatched=True))
    header = evaluation_unknowns.header
    value = replace(decision(), actor=scope.actor)

    async def concurrent_header(db, trusted):
        row = await header(db, trusted)
        await store.resolve(scope, state.version, value, confirm=True)
        return row

    monkeypatch.setattr(evaluation_unknowns, "header", concurrent_header)
    old = await store.list_unknowns(scope, state.version)
    assert old.version == state.version and len(old.executions) == 1
    monkeypatch.setattr(evaluation_unknowns, "header", header)
    with pytest.raises(CheckpointConflict):
        await store.resolve(scope, old.version, value, confirm=True)
    current = await store.list_unknowns(scope, old.version + 1)
    assert current.executions == () and not current.can_resolve


@pytest.mark.parametrize(
    "case", ["count", "boolean_count", "dispatch", "body", "release", "prior", "status"]
)
async def test_inconsistent_persisted_evidence_is_never_presented_as_a_resolution_target(
    ready, case
):
    tx, run_id, *_ = ready
    store, scope = MySQLEvaluationManagement(tx), ManagementScope(run_id, 1, 42)
    state = await recover(ready, await pending(ready, dispatched=True))
    run, _, _ = await rows(tx, run_id)
    async with tx.open() as db:
        if case in ("count", "boolean_count", "prior", "status"):
            progress = dict(run["progress_json"])
            if case == "prior":
                progress["result_unknown_resolutions"] = [
                    {
                        **asdict(decision()),
                        "execution_id": "missing",
                        "resolved_at": EXPIRY.isoformat(),
                    }
                ]
            elif case == "status":
                progress["status"] = "collecting"
            else:
                progress["unresolved_result_unknown_count"] = True if case == "boolean_count" else 0
            await db.execute(
                update(evaluation_runs)
                .where(evaluation_runs.c.run_id == str(run_id))
                .values(progress_json=progress)
            )
        elif case == "release":
            creation = json.loads(run["definition_json"])
            creation["release_fingerprint"] = "sha256:" + "0" * 64
            await db.execute(
                update(evaluation_runs)
                .where(evaluation_runs.c.run_id == str(run_id))
                .values(definition_json=json.dumps(creation))
            )
        elif case == "dispatch":
            await db.execute(
                update(evaluation_dispatches)
                .where(evaluation_dispatches.c.run_id == str(run_id))
                .values(execution_id="different")
            )
        else:
            await db.execute(
                update(evaluation_generation_completions)
                .where(evaluation_generation_completions.c.run_id == str(run_id))
                .values(normalized_output=b'{"injected":true}')
            )
        await db.commit()
    before = await snapshot(tx, run_id)
    with pytest.raises((ValueError, CheckpointConflict)):
        await store.list_unknowns(scope, state.version)
    assert await snapshot(tx, run_id) == before
