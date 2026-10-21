"""Protect original serial completion authority before readonly planner extraction."""

import json
from dataclasses import asdict, replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.persistence.mysql import evaluation_completions as original
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from tests.test_evaluation_identity import identity
from tests.test_generation_completion import completion


@pytest.fixture
def source(monkeypatch):
    run_id, value = uuid4(), completion()
    cp = ExecutionCheckpoint(
        value.execution_id,
        "generation",
        value.case_id,
        value.slot_ordinal,
        "",
        value.execution_ordinal,
        "worker:first",
        value.invocation_id,
        "dispatching",
        value.started_at,
        value.started_at + timedelta(seconds=30),
        value.started_at,
    )
    checkpoint = dict(version=5, checkpoint_json=encode(cp))
    progress = {"status": "collecting", "transitions": []}
    run = dict(
        progress_json=progress,
        definition_json=json.dumps(
            dict(
                release=asdict(identity()),
                execution_policy_json="first-frozen-policy",
                slots=[dict(case_id=value.case_id, ordinal=value.slot_ordinal)],
            )
        ),
    )
    policy = SimpleNamespace(definition_json="first-frozen-policy", generation_per_slot=2)
    monkeypatch.setattr(original, "frozen_policies", lambda _: (policy, None))
    monkeypatch.setattr(original, "validate_generation_completion", AsyncMock())
    save = AsyncMock()
    monkeypatch.setattr(original, "save_checkpoint", save)

    def rows(value):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = value
        return result

    ledger, prior = Mock(), Mock()
    ledger.scalar_one_or_none.return_value = encode(cp)
    prior.mappings.return_value.all.return_value = []
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[rows(checkpoint), rows(run), ledger, prior, Mock(), Mock()]),
        commit=AsyncMock(),
    )

    async def accept():
        return await original.complete_generation(
            db,
            run_id,
            5,
            1,
            "worker:first",
            value,
            Mock(),
            Mock(),
            candidate_id="candidate:first",
            assertions=(
                AssertionReceipt("schema", "default", 1, True, "deterministic", "passed", "Valid"),
            ),
        )

    return SimpleNamespace(**locals())


@pytest.mark.parametrize("cancel", [False, True])
async def test_original_serial_terminal_accepts_only_exact_dispatch_or_cancel_drain(source, cancel):
    p = source
    if cancel:
        p.checkpoint["version"] = 6
        p.progress["cancel_requested"] = dict(source_version=5, version=6)
    result = await p.accept()
    assert result.version == (7 if cancel else 6) and result.checkpoint is None
    p.save.assert_awaited_once_with(p.db, result, 6 if cancel else 5)
    p.db.commit.assert_not_called()


@pytest.mark.parametrize(
    "damage",
    [
        "wrong_source",
        "wrong_target",
        "missing_cancel",
        "blocked",
        "missing_cp",
        "changed_owner",
        "changed_dispatch",
    ],
)
async def test_original_serial_terminal_rejects_changed_authority_before_any_write(source, damage):
    p = source
    if damage in ("wrong_source", "wrong_target", "missing_cancel"):
        p.checkpoint["version"] = 6
        if damage != "missing_cancel":
            p.progress["cancel_requested"] = dict(
                source_version=4 if damage == "wrong_source" else 5,
                version=7 if damage == "wrong_target" else 6,
            )
    elif damage == "blocked":
        p.progress["status"] = "blocked"
    elif damage == "missing_cp":
        p.checkpoint["checkpoint_json"] = None
    elif damage == "changed_owner":
        p.checkpoint["checkpoint_json"] = encode(replace(p.cp, owner="worker:other"))
    else:
        p.ledger.scalar_one_or_none.return_value = encode(
            replace(p.cp, invocation_id="invocation:other")
        )
    with pytest.raises(CheckpointConflict):
        await p.accept()
    assert all(call.args[0].is_select for call in p.db.execute.await_args_list)
    p.save.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_planner_and_original_writer_share_authority_without_writes_or_commit(
    source, lock, cancel
):
    p = source
    if cancel:
        p.checkpoint["version"] = 6
        p.progress["cancel_requested"] = dict(source_version=5, version=6)
    read = await original.load_serial_generation_source(
        p.db, p.run_id, 5, 1, "worker:first", p.value, lock=lock
    )
    assert read.version == (6 if cancel else 5)
    assert read.checkpoint == p.cp and read.run is p.run
    assert p.db.execute.await_count == 2
    for call in p.db.execute.await_args_list:
        query = call.args[0]
        assert query.is_select
        assert (query._for_update_arg is not None) == lock
    p.save.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("lock", [False, True])
async def test_completion_read_does_not_turn_lease_expiry_into_model_retry_authority(source, lock):
    p = source
    # Existing completion acceptance is tied to immutable dispatched ownership,
    # not to the wall clock or a renewed/recreated provider call.
    expired = replace(p.cp, lease_expires_at=p.cp.claimed_at + timedelta(microseconds=1))
    p.checkpoint["checkpoint_json"] = encode(expired)
    read = await original.load_serial_generation_source(
        p.db, p.run_id, 5, 1, "worker:first", p.value, lock=lock
    )
    assert read.checkpoint == expired
    assert read.version == 5
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("lock", [False, True])
async def test_internally_valid_foreign_provider_receipt_cannot_authorize_original_dispatch(
    source, lock
):
    p = source
    assert p.value.receipt is not None
    foreign = replace(
        p.value,
        invocation_id="foreign:invocation",
        receipt=replace(p.value.receipt, invocation_id="foreign:invocation"),
    )
    # This is valid terminal evidence in isolation, but it is not the original dispatch.
    with pytest.raises(CheckpointConflict):
        await original.load_serial_generation_source(
            p.db, p.run_id, 5, 1, "worker:first", foreign, lock=lock
        )
    assert p.db.execute.await_count == 1
    p.save.assert_not_called()
    p.db.commit.assert_not_called()
