"""Drain planning keeps the original audit and does not grant cancellation/model authority."""

import copy
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from jwcrypto import jwk
from reliable_messaging.durable import MessageConflict

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql import evaluation_cancellation as original
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs
from qs_ai.infrastructure.workflow_transport import cancellation_drain_state as drain
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences


def fixture(monkeypatch):
    scope, at = ManagementScope(uuid4(), 7, 9), datetime(2026, 10, 22, tzinfo=UTC)
    request = dict(
        schema_version="qs-ai-evaluation-cancel-request/v1",
        run_id=str(scope.run_id),
        source_version=3,
        version=4,
        status="cancel_requested",
        actor=scope.actor,
        reason="original authorized request",
        requested_at=at.isoformat(),
    )
    checkpoint = dict(run_id=str(scope.run_id), version=8, checkpoint_json=None)
    run = dict(
        run_id=str(scope.run_id),
        organization_id=7,
        execution_mode="candidate_v2",
        definition_json='{"release_fingerprint":"sha256:' + "a" * 64 + '"}',
        progress_json=dict(status="collecting", transitions=[], cancel_requested=request),
    )
    previous = dict(sequence=7, version=8)

    async def execute(statement):
        result = Mock(rowcount=1)
        if statement.is_select:
            table = statement.get_final_froms()[0].name
            value = {
                evaluation_checkpoints.name: checkpoint,
                evaluation_runs.name: run,
                evaluation_sequences.name: previous,
            }[table]
            result.mappings.return_value.one.return_value = value
            result.mappings.return_value.one_or_none.return_value = value
        return result

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=execute),
        info={},
        commit=AsyncMock(),
        in_transaction=Mock(return_value=True),
        connection=AsyncMock(),
    )
    active = AsyncMock(return_value=())
    monkeypatch.setattr(original, "active_claims", active)
    record = dict(
        actor=scope.actor,
        reason=request["reason"],
        discard=False,
        canceled_at=at.isoformat(),
        execution_id="",
        source_status="collecting",
    )

    async def record_for(db, scope, current, checkpoint, decision):
        return {**record, "canceled_at": decision.canceled_at.isoformat()}

    monkeypatch.setattr(original, "record_for", record_for)
    opened = False

    @asynccontextmanager
    async def open_session():
        nonlocal opened
        opened = True
        try:
            yield db
        finally:
            opened = False

    async def header(db, actual):
        assert opened and actual == scope
        return {**run, **checkpoint}

    monkeypatch.setattr(drain, "header", header)
    seal = drain.prepare

    def outside(*args, **kwargs):
        assert not opened
        return seal(*args, **kwargs)

    prepare = Mock(side_effect=outside)
    monkeypatch.setattr(drain, "prepare", prepare)
    tx = SimpleNamespace(open=open_session)
    planner = drain.CancellationDrainPlanner(
        tx,
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
    )
    return SimpleNamespace(**locals())


async def test_drain_identity_uses_original_request_while_source_tracks_current_version(
    monkeypatch,
):
    p = fixture(monkeypatch)
    before = copy.deepcopy((p.run, p.checkpoint))
    operation = await p.planner.observe(p.scope, p.at)
    winner = await p.planner.candidate(operation, p.at + timedelta(seconds=2))
    facts, _, archive = drain.read_cancellation_drain_plan(operation, winner)
    assert operation.request_version == facts.intent.expected_version == 4
    assert facts.source_version == 8 and facts.at == p.at + timedelta(seconds=2)
    visible = pb.MessagingBody.FromString(archive.body).evaluation_state
    assert visible.status == "canceled" and visible.version == 9 and visible.event_sequence == 8
    assert (p.run, p.checkpoint) == before
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in p.db.execute.await_args_list
    )
    p.db.commit.assert_not_called()
    p.prepare.assert_called_once()
    p.checkpoint["version"] = 9
    assert (await p.planner.observe(p.scope, p.at)).intent.request == operation.intent.request
    assert operation.reason not in repr(operation)


@pytest.mark.parametrize("pending", ["claim", "unknown", "checkpoint", "done"])
async def test_pending_or_finished_drain_cannot_freeze_a_new_event(monkeypatch, pending):
    p = fixture(monkeypatch)
    if pending == "claim":
        p.active.return_value = (object(),)
    elif pending == "unknown":
        p.run["progress_json"]["unresolved_result_unknown_count"] = 1
    elif pending == "checkpoint":
        p.checkpoint["checkpoint_json"] = {"phase": "dispatching"}
    else:
        p.run["progress_json"]["status"] = "canceled"
    assert await p.planner.observe(p.scope, p.at) is None
    p.prepare.assert_not_called()
    p.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["none", "source", "late_claim", "version", "request"])
async def test_drain_root_revalidates_and_uses_original_writer_without_own_commit(
    monkeypatch, damage
):
    p = fixture(monkeypatch)
    operation = await p.planner.observe(p.scope, p.at)
    winner = await p.planner.candidate(operation, p.at)
    facts, _, _ = drain.read_cancellation_drain_plan(operation, winner)
    p.db.execute.reset_mock()
    if damage == "source":
        p.run["definition_json"] += " "
    elif damage == "late_claim":
        p.active.return_value = (object(),)
    elif damage == "version":
        p.checkpoint["version"] += 1
    elif damage == "request":
        p.request["reason"] = "different authorized request"
    if damage == "none":
        await drain.apply_cancellation_drain(p.db, facts)
        statements = [call.args[0] for call in p.db.execute.await_args_list]
        assert sum(s.is_update for s in statements) == 2
        assert all(s._for_update_arg is not None for s in statements if s.is_select)
    else:
        with pytest.raises((MessageConflict, CheckpointConflict)):
            await drain.apply_cancellation_drain(p.db, facts)
        assert all(call.args[0].is_select for call in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()
    assert p.prepare.call_count == 1


@pytest.mark.parametrize("damage", ["owner", "root_version", "context", "request_actor"])
async def test_frozen_drain_rejects_foreign_or_malformed_evidence(monkeypatch, damage):
    p = fixture(monkeypatch)
    operation = await p.planner.observe(p.scope, p.at)
    winner = await p.planner.candidate(operation, p.at)
    facts, _, _ = drain.read_cancellation_drain_plan(operation, winner)
    with pytest.raises(ValueError):
        if damage == "owner":
            drain.CancellationDrainOperation.from_facts(
                replace(facts, intent=replace(facts.intent, organization_id="8"))
            )
        elif damage == "root_version":
            drain.CancellationDrainOperation.from_facts(replace(facts, source_version=None))
        elif damage == "context":
            drain.CancellationDrainOperation.from_facts(
                replace(facts, business_context=facts.business_context + b" ")
            )
        else:
            p.request["actor"] = "user:10"
            drain.CancellationDrainOperation.from_run(p.scope, {**p.run, **p.checkpoint})
