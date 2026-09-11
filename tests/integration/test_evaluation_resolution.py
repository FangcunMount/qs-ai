import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.resolution import ResultUnknownResolution
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import accept_resolution
from qs_ai.infrastructure.persistence.mysql.evaluation_step import execute_step
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_generation_completions,
    evaluation_semantic_completions,
)
from tests.integration.test_evaluation_recovery import EXPIRY, pending, recover
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import Gateway, step
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


def decision():
    return ResultUnknownResolution(
        "execution:dead",
        "authorize_replacement",
        "user:reviewer",
        "确认重复费用后继续",
        True,
        EXPIRY,
    )


async def accept(ready, version, value=None, commit=True, org=1, confirm=True):
    tx, run_id, *_ = ready
    async with tx.open() as db:
        result = await accept_resolution(
            db, run_id, version, org, value or decision(), confirm=confirm
        )
        if commit:
            await db.commit()
    return result


@pytest.mark.parametrize("semantic", [False, True])
async def test_decision_unblocks_exact_execution_and_preserves_original(ready, semantic):
    tx, run_id, _, routes, schemas = ready
    version = 3
    if semantic:
        version = (await step(ready, Gateway(ready))).version
    state = await pending(ready, dispatched=True, version=version)
    state = await recover(ready, state)
    table = evaluation_semantic_completions if semantic else evaluation_generation_completions
    async with tx.open() as db:
        original = (
            (
                await db.execute(
                    select(table).where(
                        table.c.run_id == str(run_id), table.c.execution_id == "execution:dead"
                    )
                )
            )
            .mappings()
            .one()
        )
    state = await accept(ready, state.version)
    gateway = Gateway(ready)
    state = await execute_step(
        tx,
        run_id,
        state.version,
        1,
        "worker:after-review",
        gateway,
        routes,
        schemas,
        clock=lambda: EXPIRY + timedelta(seconds=1),
    )
    assert gateway.calls == 1
    async with tx.open() as db:
        unchanged = (
            (
                await db.execute(
                    select(table).where(
                        table.c.run_id == str(run_id), table.c.execution_id == "execution:dead"
                    )
                )
            )
            .mappings()
            .one()
        )
        all_records = (
            (await db.execute(select(table).where(table.c.run_id == str(run_id)))).mappings().all()
        )
    assert dict(original) == dict(unchanged)
    assert sorted(r["execution_ordinal"] for r in all_records) == [1, 2]
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["unresolved_result_unknown_count"] == 0
    assert len(run["progress_json"]["result_unknown_resolutions"]) == 1
    if semantic:
        assert len({r["candidate_id"] for r in all_records}) == 1


@pytest.mark.parametrize("case", ["wrong_org", "wrong_target", "unconfirmed", "stale"])
async def test_invalid_decision_does_not_change_run(ready, case):
    state = await recover(ready, await pending(ready, dispatched=True))
    tx, run_id, *_ = ready
    before = await rows(tx, run_id)
    options = {}
    if case == "wrong_org":
        options["org"] = 2
    elif case == "wrong_target":
        options["value"] = replace(decision(), execution_id="missing")
    elif case == "unconfirmed":
        options["confirm"] = False
    version = state.version - 1 if case == "stale" else state.version
    with pytest.raises((ValueError, CheckpointConflict)):
        await accept(ready, version, **options)
    assert await rows(tx, run_id) == before


async def test_rollback_and_concurrent_decisions(ready):
    state = await recover(ready, await pending(ready, dispatched=True))
    tx, run_id, *_ = ready
    before = await rows(tx, run_id)
    await accept(ready, state.version, commit=False)
    assert await rows(tx, run_id) == before
    results = await asyncio.gather(
        accept(ready, state.version), accept(ready, state.version), return_exceptions=True
    )
    assert sum(isinstance(r, CheckpointConflict) for r in results) == 1
    run, _, checkpoint = await rows(tx, run_id)
    assert len(run["progress_json"]["result_unknown_resolutions"]) == 1
    assert checkpoint["version"] == state.version + 1


async def test_cancel_keeps_unknown_evidence_but_stops_run(ready):
    state = await recover(ready, await pending(ready, dispatched=True))
    await accept(ready, state.version, replace(decision(), decision="cancel_run"))
    tx, run_id, *_ = ready
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "canceled"
    assert run["progress_json"]["transitions"][-1]["cause_code"] == "result_unknown_run_canceled"


async def test_second_unknown_cannot_be_authorized_past_budget(ready):
    tx, run_id, _, routes, schemas = ready
    state = await recover(ready, await pending(ready, dispatched=True))
    state = await accept(ready, state.version)
    state = await execute_step(
        tx,
        run_id,
        state.version,
        1,
        "worker:replacement",
        Gateway(ready, fail=True),
        routes,
        schemas,
        clock=lambda: EXPIRY + timedelta(seconds=1),
    )
    async with tx.open() as db:
        target = (
            (
                await db.execute(
                    select(evaluation_generation_completions).where(
                        evaluation_generation_completions.c.run_id == str(run_id),
                        evaluation_generation_completions.c.execution_ordinal == 2,
                    )
                )
            )
            .mappings()
            .one()
        )
    value = replace(
        decision(), execution_id=target["execution_id"], resolved_at=EXPIRY + timedelta(seconds=2)
    )
    with pytest.raises(ValueError, match="budget"):
        await accept(ready, state.version, value)
    run, _, _ = await rows(tx, run_id)
    assert run["progress_json"]["status"] == "blocked"
    assert run["progress_json"]["unresolved_result_unknown_count"] == 1
    await accept(ready, state.version, replace(value, decision="cancel_run"))


async def test_management_receipt_readback_and_organization_isolation(ready):
    from qs_ai.application.evaluation.management import ManagementScope
    from qs_ai.application.interpretation.ports import NotFound
    from qs_ai.infrastructure.persistence.mysql.evaluation_management import (
        MySQLEvaluationManagement,
    )

    tx, run_id, *_ = ready
    state = await recover(ready, await pending(ready, dispatched=True))
    store = MySQLEvaluationManagement(tx)
    scope = ManagementScope(run_id, 1, 42)
    with pytest.raises(ValueError, match="actor"):
        await store.resolve(scope, state.version, decision(), confirm=True)
    view = await store.resolve(
        scope, state.version, replace(decision(), actor=scope.actor), confirm=True
    )
    assert view.status == "collecting" and view.version == state.version + 1
    assert await store.get(scope) == view
    assert "execution:dead" in view.resolutions_json
    with pytest.raises(NotFound):
        await store.get(ManagementScope(run_id, 2, 42))
