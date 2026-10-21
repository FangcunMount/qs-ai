"""Original source snapshots never substitute for semantic candidate/dispatch authority."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import load_serial_semantic_state
from tests.test_semantic_completion import completion


def source():
    value, run_id = completion(), uuid4()
    cp = ExecutionCheckpoint(
        value.execution_id,
        "semantic",
        "case:first",
        1,
        value.candidate_id,
        1,
        "worker:first",
        value.invocation_id,
        "dispatching",
        value.started_at,
        value.started_at + timedelta(seconds=30),
        value.started_at,
    )
    checkpoint = dict(version=8, checkpoint_json=encode(cp))
    progress = dict(status="collecting")
    run = dict(run_id=str(run_id), organization_id=1, progress_json=progress)

    def rows(value):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = value
        return result

    cp_result, run_result = rows(checkpoint), rows(run)
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[cp_result, run_result]),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_original_semantic_state_scope_cancel_and_lock_boundary_without_write(lock, cancel):
    p = source()
    if cancel:
        p.checkpoint["version"] = 9
        p.progress["cancel_requested"] = dict(source_version=8, version=9)
    actual = await load_serial_semantic_state(p.db, p.run_id, 8, 1, lock=lock)
    assert actual.version == (9 if cancel else 8)
    assert actual.checkpoint == p.cp and actual.run is p.run
    calls = p.db.execute.await_args_list
    assert len(calls) == 2
    assert calls[0].args[0].compile().params["run_id_1"] == str(p.run_id)
    assert calls[1].args[0].compile().params["organization_id_1"] == 1
    assert all(call.args[0].is_select for call in calls)
    assert all((call.args[0]._for_update_arg is not None) == lock for call in calls)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize(
    "damage",
    ["missing_checkpoint", "missing_run", "wrong_source", "wrong_target", "missing_cancel"],
)
async def test_source_observation_rejects_missing_scope_or_non_cancellation_version(lock, damage):
    p = source()
    if damage == "missing_checkpoint":
        p.cp_result.mappings.return_value.one_or_none.return_value = None
    elif damage == "missing_run":
        p.run_result.mappings.return_value.one_or_none.return_value = None
    else:
        p.checkpoint["version"] = 9
        if damage != "missing_cancel":
            p.progress["cancel_requested"] = dict(
                source_version=7 if damage == "wrong_source" else 8,
                version=10 if damage == "wrong_target" else 9,
            )
    with pytest.raises(CheckpointConflict):
        await load_serial_semantic_state(p.db, p.run_id, 8, 1, lock=lock)
    assert all(call.args[0].is_select for call in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize("lock", [False, True])
async def test_scoped_state_does_not_claim_candidate_ownership_or_validate_completion(lock):
    p = source()
    p.checkpoint["checkpoint_json"] = None
    actual = await load_serial_semantic_state(p.db, p.run_id, 8, 1, lock=lock)
    assert actual.checkpoint is None  # The original caller still must reject absent ownership.
    assert actual.run is p.run and actual.version == 8
    p.db.commit.assert_not_called()
