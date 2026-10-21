"""Build the original native suite and contract bytes over future immutable assets."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.application.governance.suite_registration import RegisterSuite
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.suite_contracts import SuiteContracts
from qs_ai.infrastructure.persistence.mysql import evaluation_suites as suites
from qs_ai.infrastructure.persistence.mysql import profile_registrations as profiles
from qs_ai.infrastructure.persistence.mysql import suite_contracts
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from tests import test_profile_readonly_plan
from tests.test_generation_manifest import assets as assets
from tests.test_native_prompts import draft as draft

profile_context = test_profile_readonly_plan.planned


@pytest.fixture
async def planned(profile_context, monkeypatch):
    p = profile_context
    target = await profiles.plan_registration(
        p.db, p.scope, p.command, p.at, planned_prompt=p.prompt, planned_route=p.route
    )
    manifest = target.receipt.manifest
    command = RegisterSuite(
        uuid4(),
        V6_PUBLISHED,
        "planned-suite",
        "v1",
        manifest.profile,
        manifest.prompt,
        manifest.generation_route,
        "原套件只读规划",
    )
    execution = load_execution_policy()
    semantic = load_semantic_assets()
    bindings = SuiteContracts(
        FrozenContractRef(execution.policy_id, execution.version, execution.fingerprint),
        load_gate_policy().reference,
        semantic.prompt,
        semantic.output_schema,
    )
    source = load_suite(V6_PUBLISHED)
    prior = AsyncMock(return_value=None)
    monkeypatch.setattr(suites, "prior_receipt", prior)
    monkeypatch.setattr(suites, "load_registered_suite", AsyncMock(return_value=source))
    monkeypatch.setattr(suites, "AssetSnapshotReader", p.Reader)
    monkeypatch.setattr(suite_contracts, "read", AsyncMock(return_value=bindings))
    return SimpleNamespace(**locals())


async def preview(p, command=None, at=None):
    return await suites.plan_registration(
        p.p.db,
        p.p.scope,
        command or p.command,
        at or p.p.at,
        planned_profile=p.target.profile,
        planned_prompt=p.p.prompt,
        planned_route=p.p.route,
    )


async def test_suite_plan_preserves_original_cases_schema_and_contracts_without_writes(planned):
    p = planned
    result = await preview(p)
    assert not result.existing and result.contracts == p.bindings
    assert result.suite.manifest == p.target.receipt.manifest
    assert result.receipt.command == p.command and result.receipt.registered_at == p.p.at
    assert result.suite.reference.id == "planned-suite"
    assert result.suite.slots() == p.source.slots()
    p.p.db.execute.assert_not_called()
    p.p.db.commit.assert_not_called()
    p.p.db.rollback.assert_not_called()
    assert result.suite.definition_json not in repr(result)


@pytest.mark.parametrize("damage", ["reference", "clock", "missing", "source"])
async def test_invalid_future_suite_cannot_write_partial_asset_or_receipt(
    planned, monkeypatch, damage
):
    p = planned
    command, at = p.command, p.p.at
    if damage == "reference":
        command = replace(command, profile=replace(command.profile, content_sha256="0" * 64))
    elif damage == "clock":
        at = at.replace(tzinfo=None)
    elif damage == "missing":
        p.p.prompt = None
    else:
        monkeypatch.setattr(
            suites,
            "load_registered_suite",
            AsyncMock(side_effect=ValueError("Original suite unavailable")),
        )
    with pytest.raises(ValueError):
        await preview(p, command, at)
    p.p.db.execute.assert_not_called()
    p.p.db.commit.assert_not_called()


async def test_original_apply_keeps_exact_insert_bytes_and_caller_commit(planned, monkeypatch):
    p = planned
    result = await preview(p)
    monkeypatch.setattr(suites, "plan_registration", AsyncMock(return_value=result))
    assert await suites.apply_registration(p.p.db, p.p.scope, p.command, p.p.at) == result.receipt
    statement = p.p.db.execute.await_args.args[0]
    assert statement.table is suites.table
    values = statement.compile().params
    assert values["definition_json"] == result.suite.definition_json
    assert (
        values["receipt_json"]
        == suites.RECEIPT.dump_json(result.receipt, exclude_defaults=True).decode()
    )
    assert (values["contracts_json"], values["contracts_sha256"]) == suite_contracts.encode(
        p.bindings
    )
    p.p.db.commit.assert_not_called()
    p.p.db.rollback.assert_not_called()


async def test_original_receipt_replay_never_rebuilds_or_writes_new_asset(planned):
    p = planned
    first = await preview(p)
    p.prior.return_value = first.receipt
    replay = await preview(p, at=p.p.at.replace(year=2027))
    assert replay.existing and replay.receipt == first.receipt
    assert replay.suite is replay.contracts is None
    assert await suites.apply_registration(p.p.db, p.p.scope, p.command, p.p.at) == first.receipt
    p.p.db.execute.assert_not_called()
