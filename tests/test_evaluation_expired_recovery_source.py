"""Protect exact expiry, interrupted/unknown evidence and original-root audit before adaptation."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql import evaluation_recovery as original
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode


@pytest.fixture
def source(monkeypatch):
    run_id, at = uuid4(), datetime(2026, 10, 22, tzinfo=UTC)
    cp = ExecutionCheckpoint(
        "execution:first",
        "generation",
        "case:first",
        1,
        "",
        1,
        "worker:first",
        "invocation:first",
        "prepared",
        at,
        at + timedelta(minutes=5),
    )
    checkpoint = {"version": 7, "checkpoint_json": encode(cp)}
    progress = {"status": "collecting", "transitions": []}
    run = {"execution_mode": "serial_v1", "progress_json": progress}

    def selected(row):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = row
        return result

    ledger, current = Mock(), Mock()
    ledger.first.return_value = None
    current.scalar_one.return_value = progress
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[selected(checkpoint), selected(run), ledger, current, Mock()]
        ),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    state = CheckpointState(run_id, 8, None)
    generation, semantic, save = (
        AsyncMock(return_value=state),
        AsyncMock(return_value=state),
        AsyncMock(),
    )
    monkeypatch.setattr(original, "complete_generation", generation)
    monkeypatch.setattr(original, "complete_semantic", semantic)
    monkeypatch.setattr(original, "save_checkpoint", save)

    async def recover(**changes):
        args = dict(
            run_id=run_id,
            expected_version=7,
            organization_id=1,
            invocation_id=cp.invocation_id,
            observed_expiry=cp.lease_expires_at,
            at=cp.lease_expires_at,
            actor="system:recovery",
            routes=Mock(),
            schemas=Mock(),
        )
        args.update(changes)
        return await original.recover_expired(db, **args)

    return SimpleNamespace(**locals())


async def test_original_prepared_recovery_requires_ledger_absence_and_stages_audit_on_caller_root(
    source,
):
    p = source
    state = await p.recover()
    assert state == p.state
    p.save.assert_awaited_once_with(p.db, p.state, 7)
    p.generation.assert_not_called()
    p.semantic.assert_not_called()
    statement = p.db.execute.await_args.args[0]
    assert statement.is_update
    audit = statement.compile().params["progress_json"]["recoveries"][0]
    assert (audit["cause_code"], audit["from_version"], audit["to_version"]) == (
        "expired_preparation_released",
        7,
        8,
    )
    assert (audit["previous_owner"], audit["invocation_id"], audit["actor"]) == (
        "worker:first",
        "invocation:first",
        "system:recovery",
    )
    assert audit["observed_expiry"] == p.cp.lease_expires_at.isoformat()
    assert all(c.args[0]._for_update_arg is not None for c in p.db.execute.await_args_list[:2])
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


async def test_original_prepared_recovery_cannot_erase_committed_dispatch_evidence(source):
    p = source
    p.ledger.first.return_value = object()
    with pytest.raises(ValueError, match="dispatch evidence"):
        await p.recover()
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    p.save.assert_not_called()
    p.generation.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("kind", ["generation", "semantic"])
async def test_original_interrupted_dispatch_keeps_unknown_and_exact_first_identity(
    source, monkeypatch, kind
):
    p = source
    cp = replace(
        p.cp, kind=kind, candidate_id="candidate:first" if kind == "semantic" else ""
    ).mark_dispatching("worker:first", p.at)
    p.checkpoint["checkpoint_json"] = encode(cp)
    if kind == "semantic":
        p.ledger.mappings.return_value.one.return_value = {"first": "generation record"}
        monkeypatch.setattr(
            original,
            "decode_completion",
            lambda _: SimpleNamespace(normalized_fingerprint="sha256:" + "f" * 64),
        )
    else:
        p.db.execute.side_effect = [p.selected(p.checkpoint), p.selected(p.run), p.current, Mock()]
    assert await p.recover() == p.state
    acceptor = p.generation if kind == "generation" else p.semantic
    completion = acceptor.await_args.args[5]
    assert completion.status == "result_unknown"
    assert (
        completion.receipt is None
        and not completion.raw_output
        and not completion.normalized_output
    )
    assert (completion.execution_id, completion.invocation_id) == (
        cp.execution_id,
        cp.invocation_id,
    )
    assert completion.started_at == cp.dispatch_started_at
    assert completion.finished_at == cp.lease_expires_at
    assert completion.failure.result_unknown
    assert completion.failure.code == "execution_interrupted"
    if kind == "semantic":
        assert completion.candidate_id == cp.candidate_id
    audit = p.db.execute.await_args.args[0].compile().params["progress_json"]["recoveries"][0]
    assert audit["cause_code"] == "expired_dispatch_result_unknown"
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


@pytest.mark.parametrize(
    "wrong", ["version", "invocation", "expiry", "not_expired", "missing", "run"]
)
async def test_original_recovery_refuses_changed_authority_before_terminal_or_audit_write(
    source, wrong
):
    p = source
    args = {}
    if wrong == "version":
        args["expected_version"] = 8
    if wrong == "invocation":
        args["invocation_id"] = "invocation:other"
    if wrong == "expiry":
        args["observed_expiry"] = p.cp.lease_expires_at + timedelta(seconds=1)
    if wrong == "not_expired":
        args["at"] = p.at
    if wrong == "missing":
        p.checkpoint["checkpoint_json"] = None
    if wrong == "run":
        p.run["progress_json"] = None
    with pytest.raises(CheckpointConflict):
        await p.recover(**args)
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    p.save.assert_not_called()
    p.generation.assert_not_called()
    p.semantic.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("dispatched", [False, True])
async def test_readonly_expired_source_preserves_scope_expiry_and_has_no_terminal_authority(
    source, dispatched
):
    p = source
    cp = p.cp.mark_dispatching(p.cp.owner, p.at) if dispatched else p.cp
    p.checkpoint["checkpoint_json"] = encode(cp)
    found = await original.read_expired_recovery_source(
        p.db,
        p.run_id,
        7,
        1,
        cp.invocation_id,
        cp.lease_expires_at,
        cp.lease_expires_at,
        "system:recovery",
        lock=False,
    )
    assert found.checkpoint == cp and found.run == p.run
    assert "execution:first" not in repr(found)
    assert len(p.db.execute.await_args_list) == 2
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in p.db.execute.await_args_list
    )
    p.save.assert_not_called()
    p.generation.assert_not_called()
    p.semantic.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize(
    "wrong", ["version", "invocation", "expiry", "not_expired", "missing", "run"]
)
async def test_readonly_expiry_snapshot_still_refuses_changed_authority(source, wrong):
    p = source
    args = dict(
        expected_version=7,
        invocation_id=p.cp.invocation_id,
        observed_expiry=p.cp.lease_expires_at,
        at=p.cp.lease_expires_at,
    )
    if wrong == "version":
        args["expected_version"] = 8
    if wrong == "invocation":
        args["invocation_id"] = "invocation:other"
    if wrong == "expiry":
        args["observed_expiry"] = p.cp.lease_expires_at + timedelta(seconds=1)
    if wrong == "not_expired":
        args["at"] = p.at
    if wrong == "missing":
        p.checkpoint["checkpoint_json"] = None
    if wrong == "run":
        p.run["progress_json"] = None
    with pytest.raises(CheckpointConflict):
        await original.read_expired_recovery_source(
            p.db,
            p.run_id,
            organization_id=1,
            actor="system:recovery",
            lock=False,
            **args,
        )
    assert all(c.args[0].is_select for c in p.db.execute.await_args_list)
    p.save.assert_not_called()
    p.generation.assert_not_called()
    p.semantic.assert_not_called()
    p.db.commit.assert_not_called()
