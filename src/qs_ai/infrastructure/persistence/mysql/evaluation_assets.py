"""Resolve executable evaluation inputs from the Run's immutable asset snapshot."""

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import (
    AssetSnapshotReader,
    generation_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.schema import prompt_assets, route_assets
from qs_ai.infrastructure.qs_server.evaluation_case import (
    prepare_asset_evaluation_case,
    prepare_evaluation_case,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import load_suite
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.routes import load_route


def run_release(creation: dict[str, Any]) -> EvidenceReleaseIdentity:
    release = EvidenceReleaseIdentity(
        **{k: FrozenContractRef(**v) for k, v in creation["release"].items()}
    )
    if creation["release_fingerprint"] != release.fingerprint():
        raise ValueError("Frozen Run release fingerprint changed")
    if creation["suite_json"] != load_suite(release.suite).definition_json:
        raise ValueError("Frozen Run suite changed")
    # An incomplete manifest is corruption, never a reason to fall back to legacy assets.
    if ("generation_manifest_json" in creation) != ("generation_manifest_fingerprint" in creation):
        raise ValueError("Incomplete frozen Run manifest")
    return release


async def prepare_run_case(
    db: AsyncSession, creation: dict[str, Any], case_id: str
) -> PreparedExplanation:
    release = run_release(creation)
    if "generation_manifest_json" not in creation:
        # Retained legacy Runs can continue on their original registered baseline.
        # Existing publication gates reject these Runs without a frozen manifest.
        return prepare_evaluation_case(release, case_id)
    profile, manifest = await generation_snapshot(db, release)
    if (creation["generation_manifest_json"], creation["generation_manifest_fingerprint"]) != (
        manifest.canonical_json(),
        manifest.fingerprint(),
    ):
        raise ValueError("Run generation assets differ from frozen manifest")
    prompt = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
        manifest.prompt.identity, manifest.prompt.version
    )
    if prompt is None:
        raise ValueError("Frozen Prompt unavailable")
    policy = decode_published_profile(
        {
            "definition": json.loads(profile.definition_json),
            "fingerprint": profile.fingerprint,
            "status": "published",
        }
    )
    return prepare_asset_evaluation_case(release, case_id, policy, executable_prompt(prompt))


async def run_model_route(
    db: AsyncSession, creation: dict[str, Any], *, semantic: bool
) -> ModelRoute:
    release = run_release(creation)
    ref = release.semantic_route if semantic else release.generation_route
    if "generation_manifest_json" not in creation:
        route = load_route(ref.id, ref.version)
    else:
        asset = await AssetSnapshotReader(db, route_assets, RouteAsset).get(ref.id, ref.version)
        if asset is None:
            raise ValueError("Frozen model route unavailable")
        route = executable_route(asset)
    if route.fingerprint() != ref.fingerprint:
        raise ValueError("Execution route differs from frozen release")
    return route
