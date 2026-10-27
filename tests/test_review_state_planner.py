"""Review planning is read-only; original checks and first clocks survive execution."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.durable import MessageConflict

from qs_ai.application.evaluation.checkpoints import CheckpointConflict, CheckpointState
from qs_ai.infrastructure.workflow_transport import review_state as review
from qs_ai.infrastructure.workflow_transport.review_state_plan import read_review_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_review_state_plan import fixture as frozen


def fixture(monkeypatch):
    f = frozen()
    incoming, at = f["incoming"], f["frozen"].at
    run = dict(
        run_id=str(incoming.scope.run_id),
        organization_id=incoming.scope.organization_id,
        requested_by=incoming.scope.actor,
        execution_mode="serial_v1",
        definition_json='{"release_fingerprint":"sha256:' + "a" * 64 + '"}',
        progress_json={"status": "awaiting_review", "human_reviews": []},
        version=incoming.expected_version,
        checkpoint_json=None,
        frozen_execution_policy_fingerprint="sha256:" + "b" * 64,
        frozen_execution_policy_json="original policy",
    )
    updated = {**run["progress_json"], "human_reviews": [{"reviewed": "original"}]}
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = {"sequence": 8, "version": 9}
    db = SimpleNamespace(execute=AsyncMock(return_value=result), commit=AsyncMock())
    active, order = False, []

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    async def scoped(*args):
        assert active
        order.append("header")
        return run

    async def read(*args):
        assert active
        order.append("read_view")
        return object()

    async def project(*args, **kwargs):
        assert active and args == (db, incoming.scope, incoming.expected_version, incoming.values)
        assert kwargs == {"lock": False}
        order.append("plan")
        return CheckpointState(incoming.scope.run_id, incoming.expected_version + 1, None), updated

    monkeypatch.setattr(review, "header", AsyncMock(side_effect=scoped))
    read_view = AsyncMock(side_effect=read)
    monkeypatch.setattr(review, "read_view", read_view)
    plan_reviews = AsyncMock(side_effect=project)
    monkeypatch.setattr(review, "plan_reviews", plan_reviews)
    seal = review.prepare

    def outside(*args, **kwargs):
        assert not active
        order.append("seal")
        return seal(*args, **kwargs)

    prepare = Mock(side_effect=outside)
    monkeypatch.setattr(review, "prepare", prepare)
    tx = SimpleNamespace(open=opened)
    planner = review.ReviewStatePlanner(
        tx,
        signing_key=f["signing"],
        recipient_key=f["recipient"],
        database_seconds=0.05,
        route_resolver=legacy_routes(),
    )
    return SimpleNamespace(**locals())


async def test_full_review_plan_preserves_original_checks_source_first_clocks_and_wire(monkeypatch):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.incoming, f.at)
    facts, original, archive = read_review_state_plan(
        f.incoming, plan, route_resolver=legacy_routes()
    )
    assert facts.at == f.at and facts.sequence == 9
    assert original.snapshot_digest == review.source_snapshot_digest(f.run)
    assert original.values_for(f.incoming) == f.incoming.values
    assert f.order == ["header", "read_view", "plan", "seal"]
    assert archive.envelope().message_id == f.f["first"].envelope.message_id
    assert all(c.args[0].is_select for c in f.db.execute.await_args_list)
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["version", "active", "view", "timeout"])
async def test_planning_refuses_before_first_seal_or_business(monkeypatch, damage):
    f = fixture(monkeypatch)
    if damage == "version":
        f.run["version"] += 1
    elif damage == "active":
        f.run["checkpoint_json"] = {"active": True}
    elif damage == "view":
        f.read_view.side_effect = ValueError("Original audit invalid")
    else:

        async def stalled(*_):
            await asyncio.Event().wait()

        monkeypatch.setattr(review, "header", stalled)
    with pytest.raises((CheckpointConflict, ValueError, TimeoutError)):
        await f.planner.candidate(f.incoming, f.at)
    f.plan_reviews.assert_not_called()
    f.prepare.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("changed", [False, True])
async def test_apply_uses_original_guarded_root_and_frozen_clock_vector(monkeypatch, changed):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.incoming, f.at)
    facts, original, _ = read_review_state_plan(f.incoming, plan, route_resolver=legacy_routes())
    later = replace(
        f.incoming, values=tuple(replace(v, reviewed_at=f.at) for v in f.incoming.values)
    )
    restored = replace(later, values=original.values_for(later))
    lock = AsyncMock(return_value=f.run)
    accepted = AsyncMock()
    read = AsyncMock()
    monkeypatch.setattr(review, "lock_run_in_transaction", lock)
    monkeypatch.setattr(review, "accept_reviews", accepted)
    monkeypatch.setattr(review, "read_view", read)
    if changed:
        f.run["progress_json"] = {"status": "rejected"}
        with pytest.raises(MessageConflict):
            await review.apply_review_state(f.db, restored, facts, original)
        read.assert_not_called()
        accepted.assert_not_called()
    else:
        await review.apply_review_state(f.db, restored, facts, original)
        read.assert_awaited_once_with(f.db, f.incoming.scope, review.SERIAL_RUNTIME_LIMITS)
        accepted.assert_awaited_once_with(
            f.db, f.incoming.scope, f.incoming.expected_version, f.incoming.values
        )
    lock.assert_awaited_once_with(f.db, f.incoming.scope, f.incoming.expected_version)
    f.db.commit.assert_not_called()


async def test_review_root_binds_exact_plan_and_snapshot_before_business(monkeypatch):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.incoming, f.at)
    root = review.ReviewStateRoot(
        f.tx, object(), object(), f.incoming, plan, route_resolver=legacy_routes()
    )
    assert root._isolation_level == "REPEATABLE READ"
    assert root.view is None and root.business_error is None
    with pytest.raises(ValueError):
        review.ReviewStateRoot(
            f.tx,
            object(),
            object(),
            replace(f.incoming, expected_version=10),
            plan,
            route_resolver=legacy_routes(),
        )


@pytest.mark.parametrize("failure", [False, True])
async def test_root_callback_uses_winner_clocks_and_preserves_original_view_or_error(
    monkeypatch, failure
):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.incoming, f.at)
    facts, _, _ = read_review_state_plan(f.incoming, plan, route_resolver=legacy_routes())
    later = replace(
        f.incoming, values=tuple(replace(v, reviewed_at=f.at) for v in f.incoming.values)
    )
    expected = object()
    error = CheckpointConflict("Original review CAS changed")
    apply = AsyncMock(side_effect=error if failure else None)
    read = AsyncMock(return_value=expected)
    monkeypatch.setattr(review, "apply_review_state", apply)
    monkeypatch.setattr(review, "read_view", read)
    root = review.ReviewStateRoot(
        f.tx, object(), object(), later, plan, route_resolver=legacy_routes()
    )
    # Only callback transcription is tested here; native tests own commit proof.
    if failure:
        with pytest.raises(CheckpointConflict) as caught:
            await root._business(f.db, facts)
        assert caught.value is root.business_error is error
        assert root.view is None
        read.assert_not_called()
    else:
        await root._business(f.db, facts)
        assert root.view is expected and root.business_error is None
        read.assert_awaited_once_with(f.db, f.incoming.scope, review.SERIAL_RUNTIME_LIMITS)
    actual = apply.await_args.args[1]
    assert actual.values == f.incoming.values and actual.intent == later.intent
    f.db.commit.assert_not_called()
