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
from qs_ai.application.governance.solutions import CreateSolution, ModelSelection
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
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED


def release_from(raw: dict[str, Any]) -> EvidenceReleaseIdentity:
    return EvidenceReleaseIdentity(**{k: FrozenContractRef(**v) for k, v in raw.items()})


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
) -> AssetReference:
    source = executable_route(await route_for(db, getattr(release, purpose + "_route")))
    route = edited_route(
        source, ModelSelection(**values), f"solution-{solution_id}-{purpose}", models
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
) -> dict[str, Any]:
    """All writes share one transaction. No model invocation or publication happens here."""
    solution_id = UUID(state["solution_id"])
    release = release_from(state["source_release"])
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
    )
    semantic = await register_route(
        db,
        scope,
        solution_id,
        release,
        "semantic",
        state["semantic"],
        models,
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
            V6_PUBLISHED,
            V6_PUBLISHED.id,
            state["target_version"],
            registered.manifest.profile,
            prompt.asset,
            generation,
            state["reason"],
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
    await validate_release_assets(
        target,
        AssetSnapshotReader(db, profile_assets, ProfileAsset),
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
        frozen_suite=await load_registered_suite(db, suite.suite),
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
    )
    policy = load_execution_policy()
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
