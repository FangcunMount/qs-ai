import asyncio
from datetime import UTC, datetime

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from tests.integration.test_evaluation_runs import create, rows
from tests.integration.test_evaluation_runs import setup_run as setup_run

pytestmark = pytest.mark.integration
AT = datetime(2026, 9, 12, tzinfo=UTC)


@pytest.fixture
async def requested(setup_run):
    tx, run_id, release = setup_run
    async with tx.open() as db:
        await create(db, run_id, release)
        await db.commit()
    return tx, ManagementScope(run_id, 1, 42), MySQLEvaluationManagement(tx)


async def test_start_is_atomic_concurrent_and_preserves_frozen_creation(requested):
    tx, scope, store = requested
    before = await rows(tx, scope.run_id)
    results = await asyncio.gather(
        *(store.start(scope, 1, "确认启动评测", AT, confirm=True) for _ in range(2)),
        return_exceptions=True,
    )
    assert sum(isinstance(value, CheckpointConflict) for value in results) == 1
    view = await store.get(scope)
    assert (view.status, view.version) == ("collecting", 2)
    after = await rows(tx, scope.run_id)
    assert before[0]["definition_json"] == after[0]["definition_json"]
    assert before[1] == after[1]
    transition = after[0]["progress_json"]["transitions"][-1]
    assert transition["actor"] == "user:42"
    assert transition["reason"] == "确认启动评测"
    assert transition["cause_code"] == "evaluation_started"
    with pytest.raises(CheckpointConflict):
        await store.start(scope, 2, "禁止重复启动", AT, confirm=True)
    assert await rows(tx, scope.run_id) == after


@pytest.mark.parametrize("invalid", ["org", "version", "confirm", "reason", "time"])
async def test_invalid_start_does_not_schedule_or_change_audit(requested, invalid):
    tx, scope, store = requested
    before = await rows(tx, scope.run_id)
    kwargs = dict(expected_version=1, reason="启动", at=AT, confirm=True)
    if invalid == "org":
        scope = ManagementScope(scope.run_id, 2, 42)
    elif invalid == "version":
        kwargs["expected_version"] = 3
    elif invalid == "confirm":
        kwargs["confirm"] = False
    elif invalid == "reason":
        kwargs["reason"] = " "
    else:
        kwargs["at"] = datetime(2026, 9, 11, tzinfo=UTC)
    with pytest.raises((NotFound, ValueError, CheckpointConflict)):
        await store.start(scope, **kwargs)
    assert await rows(tx, scope.run_id) == before


@pytest.mark.parametrize("damage", ["missing", "fingerprint"])
async def test_admission_rejects_missing_or_changed_persisted_route(setup_run, damage):
    from dataclasses import replace

    from qs_ai.application.governance.solution_models import DEFAULT_EDITABLE_MODELS
    from qs_ai.infrastructure.persistence.mysql.editable_model_policy import check_editable_models

    tx, _, release = setup_run
    ref = release.generation_route
    ref = (
        replace(ref, id="unregistered-route")
        if damage == "missing"
        else replace(ref, fingerprint="sha256:" + "0" * 64)
    )
    async with tx.open() as db:
        with pytest.raises(ValueError, match="unavailable or changed"):
            await check_editable_models(
                db, replace(release, generation_route=ref), DEFAULT_EDITABLE_MODELS
            )
