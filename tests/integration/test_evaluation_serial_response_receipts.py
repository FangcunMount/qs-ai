"""Original serial receipt facts commit independently of terminal projections; no model replay."""

from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_serial_response_receipts import (
    SerialResponseReference,
    read_serial_response,
    save_serial_response,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_step import finish_step, prepare_step
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_response_receipts,
)
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
async def serial_ready(ready):
    tx, run_id, *_ = ready
    try:
        yield ready
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_response_receipts).where(
                    evaluation_response_receipts.c.run_id == str(run_id)
                )
            )
            await db.commit()


async def case(ready, semantic=False):
    tx, run_id, _, routes, schemas = ready
    version = (await step(ready, Gateway(ready))).version if semantic else 3
    prepared = await prepare_step(
        tx, run_id, version, 1, "worker:step", routes, schemas, clock=lambda: AT
    )
    gateway = Gateway(ready)
    response = await gateway.generate_messages(
        prepared.messages, prepared.route, prepared.schema, prepared.invocation_id
    )
    result = EvaluationResponse(response, None, AT + timedelta(seconds=1))
    reference = SerialResponseReference.from_prepared(1, prepared)
    return tx, run_id, routes, schemas, prepared, gateway, reference, result


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_serial_receipt_uses_original_dispatch_and_caller_commit_without_business_projection(
    serial_ready, semantic, commit
):
    tx, run_id, _, _, prepared, gateway, reference, result = await case(serial_ready, semantic)
    before = await snapshot(tx, run_id)
    async with tx.open() as db:
        await db.begin()
        root = db.get_transaction()
        await save_serial_response(db, reference, result)
        assert db.get_transaction() is root and root.is_active
        async with tx.open() as other:
            assert await read_serial_response(other, reference) is None
        assert await snapshot(tx, run_id) == before
        if commit:
            await db.commit()
    async with tx.open() as db:
        assert await read_serial_response(db, reference) == (result if commit else None)
    assert await snapshot(tx, run_id) == before and gateway.calls == 1


async def test_committed_serial_first_response_repeats_after_terminal_projection_without_overwrite(
    serial_ready,
):
    tx, run_id, routes, schemas, prepared, gateway, reference, result = await case(serial_ready)
    async with tx.open() as db:
        await db.begin()
        await save_serial_response(db, reference, result)
        await db.commit()
    await finish_step(
        tx,
        run_id,
        1,
        "worker:step",
        routes,
        schemas,
        prepared,
        result.response,
        None,
        clock=lambda: result.finished_at,
    )
    before = await snapshot(tx, run_id)
    async with tx.open() as db:
        await db.begin()
        await save_serial_response(db, reference, result)
        await db.commit()
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(CheckpointConflict, match="Immutable"):
            await save_serial_response(
                db,
                reference,
                replace(result, finished_at=result.finished_at + timedelta(seconds=1)),
            )
    async with tx.open() as db:
        assert await read_serial_response(db, reference) == result
    assert await snapshot(tx, run_id) == before and gateway.calls == 1


@pytest.mark.parametrize("wrong", ["organization", "version"])
async def test_wrong_serial_scope_or_version_never_creates_response_evidence(serial_ready, wrong):
    tx, run_id, _, _, prepared, gateway, reference, result = await case(serial_ready)
    other = (
        replace(reference, organization_id=2)
        if wrong == "organization"
        else replace(reference, version=6)
    )
    before = await snapshot(tx, run_id)
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(CheckpointConflict):
            await save_serial_response(db, other, result)
    async with tx.open() as db:
        assert await read_serial_response(db, reference) is None
    assert await snapshot(tx, run_id) == before and gateway.calls == 1


async def test_serial_response_preserves_original_explicit_cancellation_drain_identity(
    serial_ready,
):
    tx, run_id, _, _, prepared, gateway, reference, result = await case(serial_ready)
    await MySQLEvaluationManagement(tx).cancel(
        ManagementScope(run_id, 1, 42),
        prepared.state.version,
        "original cancellation drains its dispatched call",
        AT + timedelta(seconds=2),
        discard=False,
        confirm=True,
    )
    before = await snapshot(tx, run_id)
    assert before[0][2]["version"] == reference.version + 1
    async with tx.open() as db:
        await db.begin()
        await save_serial_response(db, reference, result)
        await db.commit()
    async with tx.open() as db:
        assert await read_serial_response(db, reference) == result
    assert await snapshot(tx, run_id) == before and gateway.calls == 1


async def test_serial_response_requires_the_exact_original_dispatch_ledger(serial_ready):
    tx, run_id, _, _, prepared, gateway, reference, result = await case(serial_ready)
    async with tx.open() as db:
        row = await db.scalar(
            select(evaluation_dispatches.c.checkpoint_json).where(
                evaluation_dispatches.c.run_id == str(run_id),
                evaluation_dispatches.c.invocation_id == reference.checkpoint.invocation_id,
            )
        )
        await db.execute(
            update(evaluation_dispatches)
            .where(
                evaluation_dispatches.c.run_id == str(run_id),
                evaluation_dispatches.c.invocation_id == reference.checkpoint.invocation_id,
            )
            .values(checkpoint_json={**row, "owner": "worker:foreign"})
        )
        await db.commit()
    before = await snapshot(tx, run_id)
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(CheckpointConflict, match="ledger"):
            await save_serial_response(db, reference, result)
    async with tx.open() as db:
        assert await read_serial_response(db, reference) is None
    assert await snapshot(tx, run_id) == before and gateway.calls == 1
