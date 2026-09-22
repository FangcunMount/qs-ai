"""Resolve exact sources and atomically prepare existing governance assets."""

import hashlib
import json
from dataclasses import asdict, replace
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.governance.profile_registration import RegisterProfile
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.prompt_freeze import FreezePromptDraft
from qs_ai.application.governance.solution_models import edited_route, selection
from qs_ai.application.governance.solutions import CreateSolution, ModelSelection, SaveSolution
from qs_ai.application.governance.suite_registration import RegisterSuite
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import (
    AssetSnapshotReader,
    generation_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_assets import run_release, stored_run_suite
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import (
    evaluation_contracts,
    semantic_contract,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.evaluation_snapshot import header
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import (
    apply_registration as register_suite,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.profile_registrations import (
    apply_registration as register_profile,
)
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import apply_freeze
from qs_ai.infrastructure.persistence.mysql.publication_records import load_publication
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.persistence.mysql.suite_contracts import read as read_suite_contracts
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets
from qs_ai.model_configuration import ModelConfiguration


def release_from(raw: dict[str, Any]) -> EvidenceReleaseIdentity:
    return EvidenceReleaseIdentity(**{k: FrozenContractRef(**v) for k, v in raw.items()})


def selected_release(state: dict[str, Any]) -> EvidenceReleaseIdentity:
    release = release_from(state["source_release"])
    selected = state.get("evaluation_assets")
    if not selected:
        return release
    return replace(
        release,
        **{
            field: FrozenContractRef(**selected[field])
            for field in (
                "suite",
                "execution_policy",
                "gate_policy",
                "semantic_prompt",
                "semantic_output_schema",
            )
        },
    )


async def select_evaluation_assets(
    db: AsyncSession, scope: DraftScope, state: dict[str, Any], command: SaveSolution
) -> dict[str, Any]:
    previous = selected_release(state)
    suite_ref = command.evaluation_suite or previous.suite
    suite = await load_registered_suite(db, suite_ref, organization_id=scope.organization_id)
    source_suite = await load_registered_suite(
        db, release_from(state["source_release"]).suite, organization_id=scope.organization_id
    )
    # A saved workspace cannot switch its source scene through optional suite selection.
    source_profile = json.loads(source_suite.definition_json)["profile_fixture"]
    selected_profile = json.loads(suite.definition_json)["profile_fixture"]
    for key in ("schema_version", "scene_contract_version", "selector"):
        if source_profile.get(key) != selected_profile.get(key):
            raise ValueError("Solution evaluation suite cannot change source scene")
    binding = await read_suite_contracts(db, suite_ref, scope.organization_id)
    original = release_from(state["source_release"])
    if (binding.execution_policy, binding.gate_policy, binding.semantic_output_schema) != (
        original.execution_policy,
        original.gate_policy,
        original.semantic_output_schema,
    ):
        raise ValueError("Solution cannot change execution or gate contracts")
    owner = binding.semantic_owner_organization_id
    prompt = binding.semantic_prompt
    if command.semantic_prompt is not None:
        prompt, owner = command.semantic_prompt, command.semantic_owner_organization_id
    elif command.evaluation_suite is None and state.get("evaluation_assets"):
        prompt = previous.semantic_prompt
        owner = state["evaluation_assets"]["semantic_owner_organization_id"]
    selected = replace(original, suite=suite_ref, semantic_prompt=prompt)
    await semantic_contract(db, selected, scope.organization_id, owner_organization_id=owner)
    return {
        field: asdict(getattr(selected, field))
        for field in (
            "suite",
            "execution_policy",
            "gate_policy",
            "semantic_prompt",
            "semantic_output_schema",
        )
    } | {"semantic_owner_organization_id": owner}


async def route_for(db: AsyncSession, ref: FrozenContractRef) -> RouteAsset:
    asset = await AssetSnapshotReader(db, route_assets, RouteAsset).get(ref.id, ref.version)
    if asset is None or asset.fingerprint != ref.fingerprint:
        raise ValueError("Source model route unavailable or changed")
    executable_route(asset)
    return asset


async def source_release(
    db: AsyncSession,
    scope: DraftScope,
    command: CreateSolution,
) -> EvidenceReleaseIdentity:
    if command.template_ref is not None:
        from qs_ai.infrastructure.persistence.mysql.solution_templates import template_release

        return await template_release(db, scope, command.template_ref)
    if command.publication_id:
        publication, _ = await load_publication(db, command.publication_id)
        release = publication.evidence.release
    else:
        assert command.source_run_id
        run = await header(
            db,
            ManagementScope(
                command.source_run_id,
                scope.organization_id,
                scope.operator_user_id,
            ),
            establish_snapshot=False,
        )
        creation = json.loads(run["definition_json"])
        await stored_run_suite(db, creation)
        release = run_release(creation)
    await generation_snapshot(db, release)
    return release


async def model_values(db: AsyncSession, release: EvidenceReleaseIdentity) -> dict[str, Any]:
    result = {}
    for purpose in ("generation", "semantic"):
        asset = await route_for(db, getattr(release, purpose + "_route"))
        result[purpose] = asdict(selection(executable_route(asset)))
    return result


async def register_route(
    db: AsyncSession,
    scope: DraftScope,
    solution_id: UUID,
    release: EvidenceReleaseIdentity,
    purpose: str,
    values: dict[str, Any],
    models: tuple[str, ...],
    configuration: ModelConfiguration | None = None,
) -> AssetReference:
    source = executable_route(await route_for(db, getattr(release, purpose + "_route")))
    route = edited_route(
        source,
        ModelSelection(**values),
        f"solution-{solution_id}-{purpose}",
        models,
        configuration,
        purpose,
    )
    asset = RouteAsset(route.route, route.revision, route.fingerprint(), route.definition_json())
    await db.execute(
        insert(route_assets).values(
            **asdict(asset),
            source_ref=f"qs-ai:solution:{solution_id}",
            imported_by=f"qs:org/{scope.organization_id}/user/{scope.operator_user_id}",
        )
    )
    return AssetReference(
        asset.route,
        asset.revision,
        asset.fingerprint,
        hashlib.sha256(asset.definition_json.encode()).hexdigest(),
    )


async def prepare_assets(
    db: AsyncSession,
    scope: DraftScope,
    state: dict[str, Any],
    command_id: UUID,
    at: datetime,
    models: tuple[str, ...],
    configuration: ModelConfiguration | None = None,
    execution_mode: str = "serial_v1",
) -> dict[str, Any]:
    """All writes share one transaction. No model invocation or publication happens here."""
    solution_id = UUID(state["solution_id"])
    release = selected_release(state)
    profile, manifest = await generation_snapshot(db, release)
    prompt = await apply_freeze(
        db,
        scope,
        FreezePromptDraft(
            UUID(state["draft_id"]),
            uuid5(command_id, "freeze"),
            state["draft_revision"],
            state["reason"],
        ),
        at,
    )
    generation = await register_route(
        db,
        scope,
        solution_id,
        release,
        "generation",
        state["generation"],
        models,
        configuration,
    )
    semantic = await register_route(
        db,
        scope,
        solution_id,
        release,
        "semantic",
        state["semantic"],
        models,
        configuration,
    )
    definition = json.loads(profile.definition_json)
    definition["version"] = state["target_version"]
    definition["generation_policy"].update(
        prompt_template_id=prompt.asset.identity,
        prompt_version=prompt.asset.version,
        provider_route=generation.identity,
    )
    registered = await register_profile(
        db,
        scope,
        RegisterProfile(
            uuid5(command_id, "profile"),
            manifest.profile,
            json.dumps(definition, ensure_ascii=False),
            prompt.asset,
            generation,
            state["reason"],
        ),
        at,
    )
    suite = await register_suite(
        db,
        scope,
        RegisterSuite(
            uuid5(command_id, "suite"),
            release.suite,
            release.suite.id,
            state["target_version"],
            registered.manifest.profile,
            prompt.asset,
            generation,
            state["reason"],
            semantic_prompt=release.semantic_prompt,
            semantic_owner_organization_id=state.get("evaluation_assets", {}).get(
                "semantic_owner_organization_id", 0
            ),
        ),
        at,
    )
    target = replace(
        release,
        suite=suite.suite,
        profile=FrozenContractRef(
            suite.manifest.profile.identity,
            suite.manifest.profile.version,
            suite.manifest.profile.fingerprint,
        ),
        prompt=FrozenContractRef(
            prompt.asset.identity, prompt.asset.version, prompt.asset.fingerprint
        ),
        generation_route=FrozenContractRef(
            generation.identity, generation.version, generation.fingerprint
        ),
        semantic_route=FrozenContractRef(semantic.identity, semantic.version, semantic.fingerprint),
    )
    contracts = await evaluation_contracts(db, target, scope.organization_id)
    await validate_release_assets(
        target,
        AssetSnapshotReader(db, profile_assets, ProfileAsset),
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
        frozen_suite=await load_registered_suite(
            db, suite.suite, organization_id=scope.organization_id
        ),
        execution_policy_json=contracts.execution.definition_json,
        gate_policy_json=contracts.gate.definition_json,
        semantic=contracts.semantic,
    )
    run_id = uuid5(solution_id, "evaluation")
    await create_run(
        db,
        run_id,
        target,
        scope.organization_id,
        ManagementScope(run_id, scope.organization_id, scope.operator_user_id).actor,
        state["reason"],
        at,
        generation_manifest=suite.manifest,
        execution_mode=execution_mode,
    )
    policy = contracts.execution
    return {
        "plan": {
            "generation_case_count": policy.generation_cases,
            "candidates_per_case": policy.candidates_per_case,
            "candidate_count": policy.generation_cases * policy.candidates_per_case,
            "preflight_case_count": policy.preflight_cases,
            "max_generation_invocations": policy.generation_per_run,
            "max_semantic_invocations": policy.semantic_per_run,
        },
        "run_id": str(run_id),
        "release": asdict(target),
        "manifest": asdict(suite.manifest),
        "prepared_at": at.isoformat(),
        "steps": [
            "prompt_frozen",
            "models_registered",
            "profile_registered",
            "suite_registered",
            "evaluation_requested",
        ],
    }
