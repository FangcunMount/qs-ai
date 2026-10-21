"""Future suite bindings still resolve exact original policy/semantic assets and owner."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from qs_ai.domain.evaluation.assets import PolicyAsset, PolicyKind
from qs_ai.domain.evaluation.suite_contracts import SuiteContracts
from qs_ai.infrastructure.persistence.mysql import evaluation_contracts as contracts
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release


@pytest.fixture
async def original(complete_release, monkeypatch):
    release = complete_release
    execution, gate, semantic = load_execution_policy(), load_gate_policy(), load_semantic_assets()
    binding = SuiteContracts(
        release.execution_policy,
        release.gate_policy,
        release.semantic_prompt,
        release.semantic_output_schema,
    )
    order = []

    async def read_policy(db, kind, ref):
        order.append(kind)
        if kind == PolicyKind.EXECUTION:
            assert ref == release.execution_policy
            return PolicyAsset(kind, ref, execution.definition_json)
        assert ref == release.gate_policy
        return PolicyAsset(kind, ref, gate.definition_json)

    policies = AsyncMock(side_effect=read_policy)
    reader = AsyncMock(return_value=binding)
    semantics = AsyncMock(return_value=semantic)
    monkeypatch.setattr(contracts, "read_policy", policies)
    monkeypatch.setattr(contracts, "read_suite_contracts", reader)
    monkeypatch.setattr(contracts, "semantic_contract", semantics)
    db = object()
    return SimpleNamespace(**locals())


async def test_future_binding_preserves_original_assets_without_reading_uncommitted_suite(original):
    f = original
    value = await contracts.contracts_from_binding(f.db, f.release, 1, f.binding)
    assert value == contracts.EvaluationContracts(f.execution, f.gate, f.semantic, 0)
    assert f.order == [PolicyKind.EXECUTION, PolicyKind.GATE]
    f.reader.assert_not_called()
    f.semantics.assert_awaited_once_with(f.db, f.release, 1, owner_organization_id=0)


async def test_original_registry_path_retains_exact_binding_lookup_and_asset_resolution(original):
    f = original
    value = await contracts.evaluation_contracts(f.db, f.release, 1)
    assert value == contracts.EvaluationContracts(f.execution, f.gate, f.semantic, 0)
    f.reader.assert_awaited_once_with(f.db, f.release.suite, 1)
    assert f.order == [PolicyKind.EXECUTION, PolicyKind.GATE]


@pytest.mark.parametrize(
    "damage", ["owner", "execution", "gate", "semantic", "schema", "override", "scope"]
)
async def test_future_binding_refuses_substitution_before_any_asset_or_database_read(
    original, damage
):
    f = original
    binding, org, override = f.binding, 1, None
    if damage == "owner":
        binding = replace(binding, semantic_owner_organization_id=2)
    elif damage == "override":
        override = 1
    elif damage == "scope":
        org = True
    else:
        field = {
            "execution": "execution_policy",
            "gate": "gate_policy",
            "semantic": "semantic_prompt",
            "schema": "semantic_output_schema",
        }[damage]
        binding = replace(
            binding, **{field: replace(getattr(binding, field), fingerprint="sha256:" + "f" * 64)}
        )
    with pytest.raises(ValueError):
        await contracts.contracts_from_binding(
            f.db, f.release, org, binding, semantic_owner_organization_id=override
        )
    f.reader.assert_not_called()
    f.policies.assert_not_called()
    f.semantics.assert_not_called()
