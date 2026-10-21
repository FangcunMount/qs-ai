"""Frozen governance retains authorized inputs and reads before first Seal."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from jwcrypto import jwk
from reliable_messaging.durable import MessageConflict

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.workflow_transport import evaluation_governance_state as governance
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.test_evaluation_governance_projection import approved, rejection


def fixture(monkeypatch, kind="finalize"):
    f = approved() if kind == "finalize" else rejection()
    operation = governance.GovernanceStateOperation(
        kind,
        f.scope,
        uuid4(),
        f.preview.version if kind == "finalize" else 10,
        "核对完整门槛" if kind == "finalize" else "重新核对语义分歧",
        True,
        True if kind == "finalize" else None,
    )
    at = f.preview.quality.evaluated_at if kind == "finalize" else f.at
    run = dict(
        run_id=str(f.scope.run_id),
        organization_id=f.scope.organization_id,
        requested_by=f.scope.actor,
        execution_mode="serial_v1",
        definition_json=json.dumps({"release_fingerprint": f.preview.release_fingerprint}),
        progress_json=f.progress,
        version=operation.expected_version,
        checkpoint_json=None,
        frozen_execution_policy_fingerprint="sha256:" + "b" * 64,
        frozen_execution_policy_json="original frozen policy bytes",
    )
    previous = {"sequence": 8, "version": operation.expected_version}
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = previous
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

    async def scoped(db, scope):
        assert active and scope == f.scope
        return run

    monkeypatch.setattr(governance, "header", AsyncMock(side_effect=scoped))
    monkeypatch.setattr(governance, "evaluate_snapshot", AsyncMock(return_value=f.preview))
    snapshot = f.snapshot if kind == "reopen" else None
    monkeypatch.setattr(governance, "verified_final_snapshot", AsyncMock(return_value=snapshot))
    seal = governance.prepare

    def outside(*args, **kwargs):
        assert not active
        return seal(*args, **kwargs)

    prepare = Mock(side_effect=outside)
    monkeypatch.setattr(governance, "prepare", prepare)
    tx = SimpleNamespace(open=opened)
    planner = governance.GovernanceStatePlanner(
        tx,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        database_seconds=0.05,
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
async def test_plan_freezes_first_authorized_input_clock_scope_and_final_projection(
    monkeypatch, kind
):
    f = fixture(monkeypatch, kind)
    before = copy.deepcopy(f.run)
    plan = await f.planner.candidate(f.operation, f.at)
    facts, archive = read_evaluation_state_plan(f.operation.intent, plan)
    operation, digest = governance.GovernanceStateOperation.from_facts(facts)
    assert operation == f.operation and digest == governance.source_snapshot_digest(f.run)
    assert f.run == before and facts.at == f.at
    state = pb.MessagingBody.FromString(archive.body).evaluation_state
    assert state.version == f.operation.expected_version + 1 and state.event_sequence == 9
    assert state.status == ("approved" if kind == "finalize" else "awaiting_review")
    assert plan.operation_id == str(f.operation.operation_id)
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in f.db.execute.await_args_list
    )
    f.db.commit.assert_not_called()
    f.prepare.assert_called_once()
    assert (
        replace(f.operation, reason="不同的授权输入").intent.request.input_hash
        != f.operation.intent.request.input_hash
    )


@pytest.mark.parametrize("kind", ["finalize", "reopen"])
@pytest.mark.parametrize("changed", [False, True])
async def test_original_governance_root_rechecks_source_and_keeps_commit_ownership(
    monkeypatch, kind, changed
):
    f = fixture(monkeypatch, kind)
    plan = await f.planner.candidate(f.operation, f.at)
    facts, _ = read_evaluation_state_plan(f.operation.intent, plan)
    lock = AsyncMock(return_value=f.run)
    save = AsyncMock()
    monkeypatch.setattr(governance, "lock_run_in_transaction", lock)
    monkeypatch.setattr(governance, "save_progress", save)
    if changed:
        f.run["progress_json"] = {**f.run["progress_json"], "changed_after_plan": True}
        with pytest.raises(MessageConflict):
            await governance.apply_governance_state(f.db, facts)
        save.assert_not_called()
    else:
        await governance.apply_governance_state(f.db, facts)
        lock.assert_awaited_once_with(f.db, f.f.scope, f.operation.expected_version)
        assert save.await_args.args[:3] == (f.db, f.f.scope, f.operation.expected_version)
        assert save.await_args.args[3]["status"] == (
            "approved" if kind == "finalize" else "awaiting_review"
        )
    f.db.commit.assert_not_called()


async def test_read_timeout_never_seals_or_starts_governance_business(monkeypatch):
    f = fixture(monkeypatch)

    async def stalled(*_):
        await asyncio.Event().wait()

    monkeypatch.setattr(governance, "header", stalled)
    with pytest.raises(TimeoutError):
        await f.planner.candidate(f.operation, f.at)
    f.prepare.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        {"confirm": False},
        {"kind": "unsupported"},
        {"expected_version": True},
        {"expected_version": 0},
        {"reason": " "},
        {"reason": "x" * 1001},
        {"expected_passed": None},
        {"expected_passed": 1},
    ],
)
def test_unconfirmed_or_ambiguous_governance_input_cannot_reserve(monkeypatch, changes):
    f = fixture(monkeypatch)
    with pytest.raises(ValueError):
        replace(f.operation, **changes)


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "noncanonical", "version", "intent"])
async def test_governance_context_cannot_change_authority_or_codec(monkeypatch, damage):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.operation, f.at)
    facts, _ = read_evaluation_state_plan(f.operation.intent, plan)
    raw = facts.business_context
    if damage == "duplicate":
        raw = raw[:-1] + b',"snapshot_digest":"' + b"a" * 64 + b'"}'
    elif damage == "unknown":
        raw = raw[:-1] + b',"unexpected":true}'
    elif damage == "noncanonical":
        raw = raw.replace(b'"confirm":true', b'"confirm": true')
    elif damage == "version":
        raw = raw.replace(b"/v1", b"/v2", 1)
    else:
        facts = replace(facts, intent=replace(facts.intent, operation_key="foreign/operation"))
    with pytest.raises(ValueError):
        governance.GovernanceStateOperation.from_facts(replace(facts, business_context=raw))


async def test_governance_root_binds_exact_authorized_operation_and_explicit_snapshot(monkeypatch):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.operation, f.at)
    root = governance.GovernanceStateRoot(f.tx, object(), object(), f.operation, plan)
    assert root._isolation_level == "REPEATABLE READ"
    assert root._business is governance.apply_governance_state
    with pytest.raises(ValueError):
        governance.GovernanceStateRoot(
            f.tx, object(), object(), replace(f.operation, reason="另一项授权"), plan
        )
    f.db.commit.assert_not_called()
