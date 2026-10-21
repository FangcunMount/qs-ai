"""Real readonly source selection and repeated original-root completion CAS."""

from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_completions import (
    load_serial_generation_source,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from tests.integration.test_evaluation_completions import accept, stored
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_readonly_completion_source_and_original_root_preserve_cancel_drain_and_rollback(
    dispatched, cancel, commit
):
    tx, run_id, value, *_ = dispatched
    if cancel:
        await MySQLEvaluationManagement(tx).cancel(
            ManagementScope(run_id, 1, 42),
            5,
            "Drain original dispatched completion",
            value.finished_at + timedelta(seconds=1),
            discard=False,
            confirm=True,
        )
    before = await rows(tx, run_id)
    async with tx.open() as db:
        source = await load_serial_generation_source(
            db, run_id, 5, 1, "worker:1", value, lock=False
        )
    assert source.version == (6 if cancel else 5)
    assert source.checkpoint.invocation_id == value.invocation_id
    assert await rows(tx, run_id) == before and await stored(tx, run_id) == []
    async with tx.open() as db:
        state = await accept(db, dispatched)
        assert state.version == source.version + 1
        assert state.checkpoint is None
        if commit:
            await db.commit()
        else:
            await db.rollback()
    if commit:
        assert len(await stored(tx, run_id)) == 1
        assert (await rows(tx, run_id))[2]["version"] == source.version + 1
    else:
        assert await rows(tx, run_id) == before and await stored(tx, run_id) == []


async def test_readonly_source_is_not_a_permit_to_commit_changed_dispatch(dispatched):
    tx, run_id, value, *_ = dispatched
    async with tx.open() as db:
        source = await load_serial_generation_source(
            db, run_id, 5, 1, "worker:1", value, lock=False
        )
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            assert value.receipt is not None
            foreign = replace(
                value,
                invocation_id="foreign:invocation",
                receipt=replace(value.receipt, invocation_id="foreign:invocation"),
            )
            await accept(db, dispatched, completion=foreign)
    assert await stored(tx, run_id) == []
    async with tx.open() as db:
        state = await accept(db, dispatched)
        assert state.version == source.version + 1
        await db.commit()
    assert len(await stored(tx, run_id)) == 1

    # A prior readonly observation cannot commit the same terminal fact twice.
    before = await rows(tx, run_id)
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await accept(db, dispatched)
    async with tx.open() as db:
        with pytest.raises(CheckpointConflict):
            await load_serial_generation_source(db, run_id, 5, 1, "worker:1", value, lock=False)
    assert await rows(tx, run_id) == before
    assert len(await stored(tx, run_id)) == 1
