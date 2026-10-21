"""Plan the entire original native asset graph with real immutable rules and no DML."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.governance.solution_models import selection
from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.evaluation.assets import PolicyAsset, PolicyKind, SemanticPromptAsset
from qs_ai.domain.evaluation.suite_contracts import SuiteContracts
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql import (
    evaluation_asset_registry,
    evaluation_contracts,
    evaluation_suites,
    profile_registrations,
    prompt_freezes,
    solution_assets,
    solution_preparation,
    suite_contracts,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import load_suite
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release
from tests.test_native_prompts import draft as draft


@pytest.fixture
async def original(assets, complete_release, draft, monkeypatch):
    release = complete_release
    stores, options = assets
    manifest = await build_generation_manifest(*stores, **options)
    source_profile, source_prompt, source_route = (v.get.return_value for v in stores[:3])
    execution, gate, semantic = load_execution_policy(), load_gate_policy(), load_semantic_assets()
    binding = SuiteContracts(
        release.execution_policy,
        release.gate_policy,
        release.semantic_prompt,
        release.semantic_output_schema,
    )
    scope = prompt_freezes.DraftScope(1, 42)
    ids = solution_assets.PreparationIDs.freeze(uuid4(), uuid4())
    state = dict(
        solution_id=str(ids.solution_id),
        organization_id=1,
        source_release=asdict(release),
        draft_id=str(draft.draft_id),
        draft_revision=1,
        reason="原准备意图",
        target_version=draft.target_version,
        generation=asdict(selection(executable_route(source_route))),
        semantic=asdict(selection(executable_route(source_route))),
    )
    schema_id, schema_version = semantic.output_schema.version.rsplit("/", 1)
    semantic_schema = SchemaAsset(
        schema_id, schema_version, semantic.output_schema.fingerprint, semantic.output_schema_json
    )
    registry = {
        (type(source_profile), source_profile.profile_id, source_profile.version): source_profile,
        (type(source_prompt), source_prompt.template_id, source_prompt.version): source_prompt,
        (type(source_route), source_route.route, source_route.revision): source_route,
    }
    reads = []

    class Reader:
        def __init__(self, db, table, asset_type):
            self.asset_type = asset_type

        async def get(self, identity, version):
            reads.append((self.asset_type, identity, version))
            if self.asset_type.__name__ == "SchemaAsset":
                if (identity, version) == (semantic_schema.schema_id, semantic_schema.version):
                    return semantic_schema
                return await stores[3].get(identity, version)
            return registry.get((self.asset_type, identity, version))

    for module in (
        solution_assets,
        solution_preparation,
        profile_registrations,
        evaluation_suites,
        prompt_freezes,
    ):
        monkeypatch.setattr(module, "AssetSnapshotReader", Reader)
    monkeypatch.setattr(
        solution_preparation,
        "generation_snapshot",
        AsyncMock(return_value=(source_profile, manifest)),
    )
    monkeypatch.setattr(prompt_freezes, "load_receipt", AsyncMock(return_value=None))
    monkeypatch.setattr(prompt_freezes, "read_draft", AsyncMock(return_value=draft))
    monkeypatch.setattr(profile_registrations, "load_receipt", AsyncMock(return_value=None))
    monkeypatch.setattr(evaluation_suites, "prior_receipt", AsyncMock(return_value=None))
    monkeypatch.setattr(
        evaluation_suites,
        "load_registered_suite",
        AsyncMock(return_value=load_suite(release.suite)),
    )
    monkeypatch.setattr(suite_contracts, "read", AsyncMock(return_value=binding))
    monkeypatch.setattr(
        evaluation_asset_registry,
        "read_semantic_prompt",
        AsyncMock(return_value=SemanticPromptAsset(semantic.prompt, semantic.prompt_markdown)),
    )

    async def read_policy(db, kind, ref):
        return PolicyAsset(
            kind,
            ref,
            execution.definition_json if kind == PolicyKind.EXECUTION else gate.definition_json,
        )

    monkeypatch.setattr(evaluation_contracts, "read_policy", AsyncMock(side_effect=read_policy))
    monkeypatch.setattr(evaluation_contracts, "semantic_contract", AsyncMock(return_value=semantic))
    result = Mock()
    result.first.return_value = None
    db = SimpleNamespace(
        execute=AsyncMock(return_value=result), commit=AsyncMock(), rollback=AsyncMock()
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("mode", ["serial_v1", "candidate_v2"])
async def test_complete_asset_plan_freezes_first_identities_and_original_result_without_writes(
    original, mode
):
    p = original
    plan = await solution_preparation.plan_prepare_assets(
        p.db,
        p.scope,
        p.state,
        p.ids.command_id,
        p.draft.saved_at,
        (p.source_route and executable_route(p.source_route).model,),
        execution_mode=mode,
        identities=p.ids,
    )
    assert plan.identities is p.ids and plan.creation.run_id == p.ids.run_id
    assert plan.creation.execution_mode == mode and plan.first_at == p.draft.saved_at
    assert plan.suite.receipt.manifest == plan.profile.receipt.manifest
    assert plan.creation.definition_json and len(plan.digest) == 32
    assert plan.prepared()["run_id"] == str(p.ids.run_id)
    assert plan.prepared()["prepared_at"] == p.draft.saved_at.isoformat()
    assert plan.prepared()["steps"] == [
        "prompt_frozen",
        "models_registered",
        "profile_registered",
        "suite_registered",
        "evaluation_requested",
    ]
    assert json.loads(plan.creation.definition_json)["audit"]["request_reason"] == p.state["reason"]
    assert all(call.args[0].is_select for call in p.db.execute.await_args_list)
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()
    assert p.state["reason"] not in repr(plan)
    mutable = plan.prepared()
    mutable["steps"].clear()
    assert len(plan.prepared()["steps"]) == 5


@pytest.mark.parametrize("invalid", ["scope", "mode", "clock", "identities"])
async def test_invalid_whole_plan_refuses_before_any_asset_or_write(original, invalid):
    from dataclasses import replace

    p = original
    scope, ids, at, mode = p.scope, p.ids, p.draft.saved_at, "serial_v1"
    if invalid == "scope":
        scope = replace(scope, organization_id=2)
    elif invalid == "mode":
        mode = "unsupported"
    elif invalid == "clock":
        at = at.replace(tzinfo=None)
    else:
        ids = solution_assets.PreparationIDs.freeze(uuid4(), p.ids.command_id)
    with pytest.raises(ValueError):
        await solution_preparation.plan_prepare_assets(
            p.db,
            scope,
            p.state,
            p.ids.command_id,
            at,
            ("deepseek-v4-pro",),
            execution_mode=mode,
            identities=ids,
        )
    assert not p.reads
    p.db.execute.assert_not_called()
