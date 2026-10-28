"""First Seal is outside the database; original resolver borrows the guarded root."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from jwcrypto import jwk
from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import BindingError

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.infrastructure.persistence.mysql.database import BorrowedTransactions
from qs_ai.infrastructure.persistence.mysql.evaluation_resolution import ResolutionPlan
from qs_ai.infrastructure.workflow_transport import resolution_state as resolution
from qs_ai.infrastructure.workflow_transport.evaluation_governance_dispatch import (
    GovernanceStateDispatcher,
)
from tests.route_fixtures import legacy_routes
from tests.test_resolution_state_plan import original


@pytest.fixture
def planned(monkeypatch):
    operation, _ = original()
    at = operation.value.resolved_at
    run = dict(
        run_id=str(operation.scope.run_id),
        organization_id=1,
        requested_by="user:42",
        execution_mode="serial_v1",
        definition_json='{"release_fingerprint":"sha256:' + "a" * 64 + '"}',
        progress_json={"status": "blocked", "unresolved_result_unknown_count": 1},
        version=7,
        checkpoint_json=None,
        frozen_execution_policy_fingerprint="sha256:" + "b" * 64,
        frozen_execution_policy_json="original policy",
    )
    root = SimpleNamespace(is_active=True)
    sequence = Mock()
    sequence.mappings.return_value.one_or_none.return_value = {"sequence": 2, "version": 7}
    db = SimpleNamespace(
        execute=AsyncMock(return_value=sequence),
        get_transaction=lambda: root,
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )
    active = False

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    async def header(*_, **__):
        return run

    tx = SimpleNamespace(open=opened)
    owner = SimpleNamespace(
        transactions=tx,
        capacity=object(),
        models=object(),
        quota_baseline=object(),
        runtime_limits=object(),
    )
    progress = {"status": "collecting", "unresolved_result_unknown_count": 0}
    result = ResolutionPlan(CheckpointState(operation.scope.run_id, 8, None), json.dumps(progress))
    plan = AsyncMock(return_value=result)
    admission = AsyncMock()
    monkeypatch.setattr(resolution, "header", AsyncMock(side_effect=header))
    monkeypatch.setattr(resolution, "plan_resolution", plan)
    monkeypatch.setattr(resolution, "plan_admission", admission)
    actual_seal = resolution.prepare

    def outside(*args, **kwargs):
        assert not active
        return actual_seal(*args, **kwargs)

    seal = Mock(side_effect=outside)
    monkeypatch.setattr(resolution, "prepare", seal)
    planner = resolution.ResolutionStatePlanner(
        owner,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        route_resolver=legacy_routes(),
    )
    return SimpleNamespace(**locals())


async def test_readonly_candidate_checks_original_capacity_then_seals_after_session_close(planned):
    p = planned
    winner = await p.planner.candidate(p.operation, p.at)
    facts, _ = resolution.read_resolution_plan(p.operation, winner, route_resolver=legacy_routes())
    frozen, digest = resolution.ResolutionStateOperation.from_facts(facts)
    assert frozen == p.operation and digest == resolution.source_snapshot_digest(p.run)
    assert facts.sequence == 3
    p.admission.assert_awaited_once_with(
        p.db, p.operation.scope, p.owner.capacity, p.at, p.owner.quota_baseline
    )
    p.db.commit.assert_not_called()
    p.seal.assert_called_once()


async def test_cancel_resolution_does_not_plan_replacement_capacity(planned):
    p = planned
    operation = replace(p.operation, value=replace(p.operation.value, decision="cancel_run"))
    p.plan.return_value = ResolutionPlan(
        CheckpointState(operation.scope.run_id, 8, None), '{"status":"canceled"}'
    )
    await p.planner.candidate(operation, p.at)
    p.admission.assert_not_called()


async def test_mismatched_first_clock_fails_before_reads_or_seal(planned):
    p = planned
    with pytest.raises(ValueError, match="clock"):
        await p.planner.candidate(p.operation, p.at + timedelta(seconds=1))
    p.plan.assert_not_called()
    p.seal.assert_not_called()


@pytest.mark.parametrize("changed", [False, True])
async def test_root_rechecks_source_and_borrows_original_resolver_without_committing(
    planned, monkeypatch, changed
):
    p = planned
    winner = await p.planner.candidate(p.operation, p.at)
    later = replace(
        p.operation, value=replace(p.operation.value, resolved_at=p.at + timedelta(days=1))
    )
    executor = resolution.ResolutionStateRoot(
        p.owner, object(), object(), later, winner, route_resolver=legacy_routes()
    )
    facts, _ = resolution.read_resolution_plan(p.operation, winner, route_resolver=legacy_routes())
    admitted = AsyncMock()
    resolver = SimpleNamespace(resolve=AsyncMock(return_value=object()))
    factory = Mock(return_value=resolver)
    monkeypatch.setattr(resolution, "lock_admission", admitted)
    monkeypatch.setattr(resolution, "MySQLEvaluationManagement", factory)
    if changed:
        p.run["progress_json"] = {"status": "canceled"}
        with pytest.raises(MessageConflict, match="source"):
            await executor._business(p.db, facts)
        factory.assert_not_called()
        assert executor.view is None
    else:
        await executor._business(p.db, facts)
        args = factory.call_args.args
        assert isinstance(args[0], BorrowedTransactions) and args[0].session is p.db
        assert args[1:] == (
            p.owner.capacity,
            p.owner.models,
            p.owner.quota_baseline,
            p.owner.runtime_limits,
        )
        resolver.resolve.assert_awaited_once_with(
            p.operation.scope, 7, p.operation.value, confirm=True
        )
        assert executor.view is resolver.resolve.return_value
        assert executor._isolation_level == "REPEATABLE READ"
    admitted.assert_awaited_once_with(p.db, p.operation.scope.organization_id)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()


async def test_missing_original_owner_refuses_resolution_before_lookup_or_any_business(planned):
    p = planned
    store = SimpleNamespace(lookup=AsyncMock())
    dispatch = GovernanceStateDispatcher(
        p.tx,
        object(),
        SimpleNamespace(routing=legacy_routes(), store=store),
        signing_key=object(),
        recipient_key=object(),
    )
    with pytest.raises(BindingError, match="binding"):
        await dispatch(p.operation, p.at)
    store.lookup.assert_not_called()


def test_incorrect_original_owner_is_refused_by_inert_constructor(planned):
    p = planned
    with pytest.raises(ValueError, match="owner"):
        GovernanceStateDispatcher(
            object(),
            object(),
            object(),
            signing_key=object(),
            recipient_key=object(),
            resolution_management=p.owner,
        )


async def test_root_preserves_original_version_conflict_before_source_digest(planned, monkeypatch):
    p = planned
    winner = await p.planner.candidate(p.operation, p.at)
    executor = resolution.ResolutionStateRoot(
        p.owner, object(), object(), p.operation, winner, route_resolver=legacy_routes()
    )
    facts, _ = resolution.read_resolution_plan(p.operation, winner, route_resolver=legacy_routes())
    monkeypatch.setattr(resolution, "lock_admission", AsyncMock())
    p.run["version"] += 1
    with pytest.raises(CheckpointConflict, match="version"):
        await executor._business(p.db, facts)
    assert isinstance(executor.business_error, CheckpointConflict)
    assert executor.view is None
