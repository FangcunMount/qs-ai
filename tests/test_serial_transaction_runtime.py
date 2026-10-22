"""Original serial response ownership and a separate completion boundary."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import BindingError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.infrastructure.persistence.mysql.database import changed_evaluation
from qs_ai.infrastructure.workflow_transport import serial_runtime as module
from tests.test_evaluation_serial_response_receipts import original as original


def runtime_rig(tx):
    transport, archives = object(), object()
    dispatch = SimpleNamespace(_transactions=tx, _runtime=transport, _archives=archives)
    terminals = []
    for _ in range(2):
        terminal = AsyncMock()
        terminal._transactions, terminal._runtime, terminal._archives = tx, transport, archives
        terminal._planner = SimpleNamespace(database_seconds=2)
        terminals.append(terminal)
    generation, semantic = terminals
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("different", ["transaction", "transport", "archives", "recovery"])
def test_serial_runtime_rejects_mixed_original_owners_without_starting_any_resource(different):
    r = runtime_rig(Mock())
    recovery = SimpleNamespace(sender=object()) if different == "recovery" else None
    if different != "recovery":
        field = {"transaction": "_transactions", "transport": "_runtime", "archives": "_archives"}[
            different
        ]
        setattr(r.semantic, field, object())
    with pytest.raises(ValueError, match="Serial"):
        module.SerialRuntime(r.dispatch, r.generation, r.semantic, dispatch_recovery=recovery)
    r.generation.assert_not_called()
    r.semantic.assert_not_called()


async def test_serial_response_guard_rejects_business_event_staging_and_inactive_root():
    async with AsyncSession() as db:
        with pytest.raises(BindingError):
            module._SerialResponseEvents(db)
        await db.begin()
        events = module._SerialResponseEvents(db)
        db.info["state_events"] = events
        await events.flush(db)
        with pytest.raises(BindingError):
            await events.record_interpretation(db, {})
        await db.rollback()
        with pytest.raises(BindingError):
            await events.flush(db)


@pytest.mark.parametrize(
    "fault", ["foreign_session", "nested", "events_replaced", "business_changed"]
)
async def test_serial_response_guard_never_suppresses_a_business_change_or_crosses_root(fault):
    async with AsyncSession() as db, AsyncSession() as other:
        await db.begin()
        events = module._SerialResponseEvents(db)
        db.info["state_events"] = events
        if fault == "nested":
            await db.begin_nested()
        elif fault == "events_replaced":
            db.info["state_events"] = object()
        elif fault == "business_changed":
            changed_evaluation(db, "run:changed")
        with pytest.raises(BindingError):
            await events.flush(other if fault == "foreign_session" else db)


@pytest.mark.parametrize("kind", ["generation", "semantic"])
@pytest.mark.parametrize("evidence", ["exact", "missing", "changed"])
async def test_serial_completion_closes_scoped_first_response_read_before_terminal_sender(
    original, monkeypatch, kind, evidence
):
    p = original
    if kind == "semantic":
        p.ref = replace(
            p.ref, checkpoint=replace(p.cp, kind="semantic", candidate_id="candidate:first")
        )
    prepared = SimpleNamespace()
    # This unit tests composition, not the separately tested prepared identity codec.
    monkeypatch.setattr(module.SerialResponseReference, "from_prepared", lambda *args: p.ref)
    db = object()
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    r = runtime_rig(SimpleNamespace(open=opened))
    runtime = module.SerialRuntime(r.dispatch, r.generation, r.semantic)
    first = p.result if evidence == "exact" else None
    if evidence == "changed":
        first = replace(p.result, finished_at=p.result.finished_at.replace(microsecond=1))
    read = AsyncMock(return_value=first)
    monkeypatch.setattr(module, "read_serial_response", read)

    async def terminal(reference):
        assert not active and reference == p.ref
        return CheckpointState(p.ref.run_id, 6, None)

    chosen = r.generation if kind == "generation" else r.semantic
    chosen.side_effect = terminal
    if evidence == "exact":
        assert (await runtime.complete(1, prepared, p.result)).version == 6
        chosen.assert_awaited_once_with(p.ref)
    else:
        with pytest.raises(MessageConflict):
            await runtime.complete(1, prepared, p.result)
        r.generation.assert_not_called()
        r.semantic.assert_not_called()
    read.assert_awaited_once_with(db, p.ref)
