"""Plan original worker preflight read-only, Seal after closing, execute frozen evidence."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from jwcrypto import jwk
from reliable_messaging.durable import MessageConflict

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite
from qs_ai.infrastructure.workflow_transport import evaluation_preflight_state as preflight
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import read_evaluation_state_plan
from tests.route_fixtures import legacy_routes
from tests.test_evaluation_preflight_plan import fixture as source


def selected(row):
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    return result


def fixture(monkeypatch, previous=None):
    f = source()
    suite = load_suite(V6_PUBLISHED)
    creation = {
        **f.creation,
        "preflight": {"case_id": suite.preflight_case_id},
        "release": {"suite": asdict(suite.reference)},
        "release_fingerprint": "sha256:" + "a" * 64,
    }
    f.run["definition_json"] = json.dumps(creation)
    intent = preflight.preflight_intent(f.run_id, 2, 7)
    active = False
    db = SimpleNamespace(
        connection=AsyncMock(),
        execute=AsyncMock(
            side_effect=[selected(f.checkpoint), selected(f.run), selected(previous)]
        ),
        commit=AsyncMock(),
    )

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield db
        finally:
            active = False

    async def stored(original, _):
        assert original is db and active
        return suite

    monkeypatch.setattr(preflight, "stored_run_suite", AsyncMock(side_effect=stored))
    real_seal = preflight.prepare

    def seal(*args, **kwargs):
        assert not active, "first Seal must follow read-transaction cleanup"
        return real_seal(*args, **kwargs)

    prepare = Mock(side_effect=seal)
    monkeypatch.setattr(preflight, "prepare", prepare)

    def identity():
        assert not active
        return str(uuid4())

    planner = preflight.PreflightStatePlanner(
        SimpleNamespace(open=opened),
        signing_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign"),
        recipient_key=jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt"),
        clock=lambda: f.at,
        new_id=identity,
        database_seconds=0.05,
        route_resolver=legacy_routes(),
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("previous", [None, {"sequence": 8, "version": 2}])
async def test_original_preflight_plan_freezes_evidence_and_state_without_writes_or_model(
    monkeypatch, previous
):
    f = fixture(monkeypatch, previous)
    plan = await f.planner.candidate(f.intent)
    facts, value = read_evaluation_state_plan(f.intent, plan, route_resolver=legacy_routes())
    original = preflight.PreflightOperation.from_facts(facts)
    assert original.evidence.provider_call_count == 0 and original.evidence.status == "passed"
    assert original.evidence.evaluated_at == facts.at == f.f.at
    assert original.snapshot_digest == preflight.source_snapshot_digest(f.f.checkpoint, f.f.run)
    state = pb.MessagingBody.FromString(value.body).evaluation_state
    assert (state.version, state.event_sequence, state.status) == (
        3,
        9 if previous else 1,
        "collecting",
    )
    assert all(
        c.args[0].is_select and c.args[0]._for_update_arg is None
        for c in f.db.execute.await_args_list
    )
    f.db.commit.assert_not_called()
    assert f.prepare.call_count == 1


async def test_database_read_budget_does_not_seal_or_execute_business(monkeypatch):
    f = fixture(monkeypatch)

    async def locked(*_, **__):
        await asyncio.Event().wait()

    monkeypatch.setattr(preflight, "load_preflight_source", locked)
    with pytest.raises(TimeoutError):
        await f.planner.candidate(f.intent)
    f.prepare.assert_not_called()
    f.db.commit.assert_not_called()


@pytest.mark.parametrize("lock", [True, False])
async def test_same_original_source_loader_can_plan_read_only_and_lock_for_execution(
    monkeypatch, lock
):
    f = fixture(monkeypatch)
    checkpoint, run = await preflight.load_preflight_source(f.db, f.intent, lock=lock)
    assert checkpoint == f.f.checkpoint and run == f.f.run
    assert all(
        (c.args[0]._for_update_arg is not None) is lock for c in f.db.execute.await_args_list
    )


@pytest.mark.parametrize("changed", [False, True])
async def test_original_executor_uses_winning_preflight_evidence_and_rechecks_snapshot(
    monkeypatch, changed
):
    f = fixture(monkeypatch)
    plan = await f.planner.candidate(f.intent)
    facts, _ = read_evaluation_state_plan(f.intent, plan, route_resolver=legacy_routes())
    operation = preflight.PreflightOperation.from_facts(facts)
    if changed:
        f.f.run["progress_json"] = {"status": "blocked"}
    f.db.execute.side_effect = [selected(f.f.checkpoint), selected(f.f.run)]
    complete = AsyncMock()
    monkeypatch.setattr(preflight, "complete_preflight", complete)
    if changed:
        with pytest.raises(MessageConflict):
            await preflight.apply_preflight_state(f.db, facts)
        complete.assert_not_called()
    else:
        await preflight.apply_preflight_state(f.db, facts)
        complete.assert_awaited_once_with(f.db, f.f.run_id, 2, 7, operation.evidence)
    f.db.commit.assert_not_called()
