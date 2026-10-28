"""Correction planning and historical replay are read-only; roots keep first audit time."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.infrastructure.workflow_transport import correction_state as correction
from qs_ai.infrastructure.workflow_transport.correction_state_plan import CorrectionStateOperation
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_correction_state_operation import fixture as original


def fixture(monkeypatch):
    operation, _ = original()
    at = operation.value.reviewed_at
    run = dict(
        run_id=str(operation.scope.run_id),
        organization_id=7,
        requested_by="user:42",
        execution_mode="serial_v1",
        definition_json='{"release_fingerprint":"sha256:' + "a" * 64 + '"}',
        progress_json={"status": "awaiting_review", "review_corrections": []},
        version=operation.expected_version,
        checkpoint_json=None,
        frozen_execution_policy_fingerprint="sha256:" + "b" * 64,
        frozen_execution_policy_json="original policy",
    )
    updated = {**run["progress_json"], "review_corrections": [{"original": "frozen"}]}
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = {
        "sequence": 8,
        "version": operation.expected_version,
    }
    db = SimpleNamespace(execute=AsyncMock(return_value=result), commit=AsyncMock())
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    async def header(*args):
        assert active
        return run

    plan = AsyncMock(return_value=updated)
    read = AsyncMock(return_value=object())
    replay = Mock(return_value=False)
    monkeypatch.setattr(correction, "header", AsyncMock(side_effect=header))
    monkeypatch.setattr(correction, "plan_review_correction", plan)
    monkeypatch.setattr(correction, "correction_replayed", replay)
    monkeypatch.setattr(correction, "read_view", read)
    seal = correction.prepare

    def outside(*args, **kwargs):
        assert not active
        return seal(*args, **kwargs)

    prepare = Mock(side_effect=outside)
    monkeypatch.setattr(correction, "prepare", prepare)
    from jwcrypto import jwk

    tx = SimpleNamespace(open=opened)
    planner = correction.CorrectionStatePlanner(
        tx,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        route_resolver=legacy_routes(),
    )
    return SimpleNamespace(**locals())


async def test_candidate_binds_complete_original_input_before_first_seal(monkeypatch):
    f = fixture(monkeypatch)
    winner = await f.planner.candidate(f.operation, f.at)
    facts, archive = read_evaluation_state_plan(
        f.operation.intent, winner, route_resolver=legacy_routes()
    )
    actual, digest = CorrectionStateOperation.from_facts(facts)
    assert actual == f.operation and digest == correction.source_snapshot_digest(f.run)
    assert winner.operation_id == f.operation.command_id and facts.sequence == 9
    assert archive.wire and f.prepare.call_count == 1
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("replayed", [False, True])
async def test_historical_business_replay_never_seals_or_constructs_transaction_evidence(
    monkeypatch, replayed
):
    f = fixture(monkeypatch)
    f.replay.return_value = replayed
    assert await f.planner.replay(f.operation) is (f.read.return_value if replayed else None)
    assert f.read.await_count == int(replayed)
    f.plan.assert_not_called()
    f.prepare.assert_not_called()
    f.db.commit.assert_not_called()


async def test_race_with_already_committed_original_command_has_no_new_event_plan(monkeypatch):
    f = fixture(monkeypatch)
    f.plan.return_value = None
    assert await f.planner.candidate(f.operation, f.at) is None
    f.prepare.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", [None, "source", "replay"])
async def test_original_root_rechecks_source_and_only_saves_new_frozen_correction(
    monkeypatch, damage
):
    f = fixture(monkeypatch)
    winner = await f.planner.candidate(f.operation, f.at)
    facts, _ = read_evaluation_state_plan(
        f.operation.intent, winner, route_resolver=legacy_routes()
    )
    lock = AsyncMock(return_value=f.run)
    save = AsyncMock()
    monkeypatch.setattr(correction, "lock_run_in_transaction", lock)
    monkeypatch.setattr(correction, "save_progress", save)
    if damage == "source":
        f.run["progress_json"] = {"status": "rejected"}
    elif damage == "replay":
        f.plan.return_value = None
    if damage:
        with pytest.raises(MessageConflict):
            await correction.apply_correction_state(f.db, facts)
        save.assert_not_called()
    else:
        await correction.apply_correction_state(f.db, facts)
        save.assert_awaited_once_with(
            f.db, f.operation.scope, f.operation.expected_version, f.updated
        )
    f.db.commit.assert_not_called()


async def test_executor_restores_winner_audit_time_before_root_callback(monkeypatch):
    f = fixture(monkeypatch)
    winner = await f.planner.candidate(f.operation, f.at)
    later = replace(
        f.operation, value=replace(f.operation.value, reviewed_at=f.at + timedelta(seconds=5))
    )
    root = correction.CorrectionStateRoot(
        f.tx, object(), object(), later, winner, route_resolver=legacy_routes()
    )
    assert root._isolation_level == "REPEATABLE READ" and root.view is None
    facts, _ = read_evaluation_state_plan(
        f.operation.intent, winner, route_resolver=legacy_routes()
    )
    apply = AsyncMock()
    monkeypatch.setattr(correction, "apply_correction_state", apply)
    await root._business(f.db, facts)
    actual, _ = CorrectionStateOperation.from_facts(apply.await_args.args[1])
    assert actual == f.operation and actual.value != later.value
    assert root.view is f.read.return_value
    f.db.commit.assert_not_called()
