"""Read-only source planning, first Seal and original same-root business ownership."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from jwcrypto import jwk
from reliable_messaging.durable import MessageConflict

from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions, SolutionApplyResult
from qs_ai.infrastructure.workflow_transport import solution_preparation_state as preparation
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationInput,
    read_preparation_plan,
)
from tests.route_fixtures import legacy_routes
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_native_prompts import draft as draft
from tests.test_solution_preparation_plan import original as original


@pytest.fixture
async def planning(original, monkeypatch):
    p = original
    incoming = SolutionPreparationInput(
        p.scope,
        p.ids.solution_id,
        PrepareSolution(
            command_id=p.ids.command_id,
            reason=p.state["reason"],
            expected_revision=1,
        ),
    )
    p.state.update(revision=1, prepared=None, reason="原先保存的原因")
    p.db.connection = AsyncMock()
    active, order = False, []

    @asynccontextmanager
    async def opened():
        nonlocal active
        active = True
        try:
            yield p.db
        finally:
            active = False
            order.append("closed")

    from qs_ai.application.interpretation.route_assets import executable_route

    settings = SimpleNamespace(
        governance_models=(executable_route(p.source_route).model,),
        models=None,
        evaluation=SimpleNamespace(candidate_mode_enabled=True),
    )
    store = MySQLSolutions(SimpleNamespace(open=opened), settings)
    checked = AsyncMock(side_effect=lambda *_: order.append("checked"))
    monkeypatch.setattr(store, "_check_selections", checked)
    saved = AsyncMock(return_value=None)
    source, draft_read = AsyncMock(return_value=p.state), AsyncMock(return_value=p.draft)
    preview = AsyncMock(side_effect=lambda db, scope, state: {**state, "original_hydrated": True})
    monkeypatch.setattr(preparation, "prior", saved)
    monkeypatch.setattr(preparation, "read_state", source)
    monkeypatch.setattr(preparation, "read_draft", draft_read)
    monkeypatch.setattr(preparation, "hydrate", preview)
    seal = preparation.prepare

    def outside(*args, **kwargs):
        assert not active
        order.append("sealed")
        return seal(*args, **kwargs)

    first = Mock(side_effect=outside)
    monkeypatch.setattr(preparation, "prepare", first)
    signing = jwk.JWK.generate(kty="EC", crv="P-256", kid="ai.sign")
    recipient = jwk.JWK.generate(kty="EC", crv="P-256", kid="qs.encrypt")
    planner = preparation.SolutionPreparationPlanner(
        store, signing_key=signing, recipient_key=recipient, route_resolver=legacy_routes()
    )
    return SimpleNamespace(**locals())


async def test_source_and_whole_assets_are_frozen_before_read_closes_and_first_seal(planning):
    f = planning
    candidate = await f.planner.candidate(f.incoming, f.p.draft.saved_at)
    facts, original, _ = read_preparation_plan(
        f.incoming, candidate.winner, route_resolver=legacy_routes()
    )
    assert original.source_digest == preparation.source_digest(candidate.source_json)
    assert original.draft_digest == preparation.draft_digest(f.p.draft)
    original.validate_assets(f.incoming, candidate.assets)
    assert json.loads(candidate.source_json)["reason"] == "原先保存的原因"
    target = json.loads(candidate.target_json)
    assert target["revision"] == 2 and target["reason"] == f.incoming.command.reason
    assert target["prepared"] == candidate.assets.prepared()
    assert candidate.assets.creation.execution_mode == "candidate_v2"
    assert f.order == ["checked", "closed", "sealed"]
    assert all(call.args[0].is_select for call in f.p.db.execute.await_args_list)
    f.p.db.commit.assert_not_called()
    f.p.db.rollback.assert_not_called()


@pytest.mark.parametrize("damage", ["revision", "prepared", "draft", "models", "response"])
async def test_invalid_original_source_never_seals_or_writes(planning, damage):
    f = planning
    if damage == "revision":
        f.p.state["revision"] = 2
    elif damage == "prepared":
        f.p.state["prepared"] = {"run_id": "old"}
    elif damage == "draft":
        f.draft_read.return_value = replace(f.p.draft, revision=2)
    elif damage == "models":
        f.checked.side_effect = ValueError("Original model no longer allowed")
    else:
        f.preview.return_value = {"oversize": "x" * 1048577}
        f.preview.side_effect = None
    with pytest.raises(ValueError):
        await f.planner.candidate(f.incoming, f.p.draft.saved_at)
    f.first.assert_not_called()
    assert all(call.args[0].is_select for call in f.p.db.execute.await_args_list)
    f.p.db.commit.assert_not_called()


async def test_historical_receipt_has_no_new_candidate_or_first_wire(planning):
    f = planning
    f.saved.return_value = f.p.state
    assert await f.planner.candidate(f.incoming, f.p.draft.saved_at) is None
    f.source.assert_not_called()
    f.first.assert_not_called()
    assert await f.planner.replay(f.incoming) == {**f.p.state, "original_hydrated": True}
    f.p.db.commit.assert_not_called()


@pytest.mark.parametrize("damage", ["source", "target"])
async def test_root_refuses_substituted_frozen_candidate_before_any_business(planning, damage):
    f = planning
    candidate = await f.planner.candidate(f.incoming, f.p.draft.saved_at)
    changed = replace(
        candidate,
        **(
            {"source_json": candidate.source_json + " "}
            if damage == "source"
            else {"target_json": candidate.target_json + " "}
        ),
    )
    with pytest.raises(MessageConflict):
        preparation.SolutionPreparationRoot(
            f.store,
            object(),
            object(),
            f.incoming,
            candidate.winner,
            changed,
            route_resolver=legacy_routes(),
        )


@pytest.mark.parametrize("damage", ["none", "source", "draft", "replay", "result"])
async def test_root_uses_same_session_first_facts_and_captured_response_only(
    planning, monkeypatch, damage
):
    f = planning
    candidate = await f.planner.candidate(f.incoming, f.p.draft.saved_at)
    facts, original, _ = read_preparation_plan(
        f.incoming, candidate.winner, route_resolver=legacy_routes()
    )
    response = json.loads(candidate.target_json)
    applied = AsyncMock(return_value=SolutionApplyResult(response, True))
    monkeypatch.setattr(f.store, "apply_in", applied)
    root = preparation.SolutionPreparationRoot(
        f.store,
        object(),
        object(),
        f.incoming,
        candidate.winner,
        candidate,
        route_resolver=legacy_routes(),
    )
    verified = AsyncMock()
    monkeypatch.setattr(root, "_verify_business", verified)
    if damage == "source":
        f.source.return_value = {**f.p.state, "revision": 2}
    elif damage == "draft":
        f.draft_read.return_value = replace(f.p.draft, reason="changed saved source")
    elif damage == "replay":
        applied.return_value = SolutionApplyResult(response, False)
    elif damage == "result":
        response["prepared"]["run_id"] = "different"
    assert root._isolation_level == "READ COMMITTED"
    if damage != "none":
        with pytest.raises((DraftConflict, MessageConflict)):
            await root._business(f.p.db, facts)
        assert root.value is None and root.business_error is not None
        verified.assert_not_called()
        if damage in ("source", "draft"):
            applied.assert_not_called()
    else:
        await root._business(f.p.db, facts)
        assert root.value is response and root.business_error is None
        verified.assert_awaited_once_with(f.p.db)
        applied.assert_awaited_once_with(
            f.p.db,
            f.incoming.scope,
            f.incoming.solution_id,
            f.incoming.command,
            original.first_at,
            preparation_ids=candidate.assets.identities,
            execution_mode="candidate_v2",
        )
    f.p.db.commit.assert_not_called()
    f.p.db.rollback.assert_not_called()
