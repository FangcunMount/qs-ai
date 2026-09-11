import hashlib
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from qs_ai.application.interpretation.manifest import ManifestUnavailable, build_generation_manifest
from qs_ai.bootstrap.import_profiles import baseline_assets as profile_baseline
from qs_ai.bootstrap.import_prompts import baseline_assets as prompt_baseline
from qs_ai.bootstrap.import_routes import baseline_assets as route_baseline
from qs_ai.bootstrap.import_schemas import baseline_assets as schema_baseline


@pytest.fixture
def assets():
    stores = [AsyncMock() for _ in range(4)]
    profile = profile_baseline()[1][0]
    prompt = prompt_baseline()[1][-1]
    route = route_baseline()[1][0]
    schemas = schema_baseline()[1]
    for store, value in zip(stores[:3], (profile, prompt, route), strict=True):
        store.get.return_value = value
    by_key = {(a.schema_id, a.version): a for a in schemas}
    stores[3].get.side_effect = lambda identity, version: by_key.get((identity, version))
    return stores, {
        "profile_id": profile.profile_id,
        "profile_version": profile.version,
        "route_revision": route.revision,
    }


async def test_manifest_binds_exact_versions_source_fingerprints_and_package_bytes(assets):
    stores, selection = assets
    manifest = await build_generation_manifest(*stores, **selection)
    assert manifest == await build_generation_manifest(*stores, **selection)
    assert manifest.profile.version == "v6"
    assert manifest.prompt.version == "v6"
    assert manifest.generation_route.version == "v8"
    assert manifest.input_schema.version == manifest.output_schema.version == "v1"
    # Same source Prompt fingerprint cannot conceal a changed exported package.
    prompt = stores[1].get.return_value
    raw = prompt.package_json + " "
    stores[1].get.return_value = replace(
        prompt, package_json=raw, package_sha256=hashlib.sha256(raw.encode()).hexdigest()
    )
    changed = await build_generation_manifest(*stores, **selection)
    assert changed.prompt.fingerprint == manifest.prompt.fingerprint
    assert changed.fingerprint() != manifest.fingerprint()
    for store in stores:
        store.put.assert_not_called()


@pytest.mark.parametrize("missing", range(4))
async def test_missing_assets_never_produce_partial_manifest(assets, missing):
    stores, selection = assets
    stores[missing].get.side_effect = None
    stores[missing].get.return_value = None
    with pytest.raises(ManifestUnavailable):
        await build_generation_manifest(*stores, **selection)


async def test_route_revision_is_explicit_and_cannot_silently_fall_back(assets):
    stores, selection = assets
    selection["route_revision"] = "v9"
    with pytest.raises(ManifestUnavailable, match="Route"):
        await build_generation_manifest(*stores, **selection)
    stores[2].get.assert_awaited_once_with("balanced_text_v1", "v9")


@pytest.fixture
async def evaluation_release(assets):
    from dataclasses import fields

    from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef

    stores, selection = assets
    manifest = await build_generation_manifest(*stores, **selection)
    refs = {
        f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
        for f in fields(EvidenceReleaseIdentity)
    }
    for name in ("profile", "prompt", "generation_route", "input_schema", "output_schema"):
        asset = getattr(manifest, name)
        version = f"{asset.identity}/{asset.version}" if name.endswith("schema") else asset.version
        refs[name] = FrozenContractRef(asset.identity, version, asset.fingerprint)
    return EvidenceReleaseIdentity(**refs)


async def test_evaluation_resolves_generation_assets_with_original_full_schema_versions(
    assets, evaluation_release
):
    from qs_ai.application.evaluation.release import resolve_generation_assets

    stores, selection = assets
    resolved = await resolve_generation_assets(evaluation_release, *stores)
    assert resolved == await build_generation_manifest(*stores, **selection)
    assert evaluation_release.input_schema.version == "ai-explanation-input/v1"
    assert evaluation_release.output_schema.version == "ai-explanation-output/v1"


@pytest.mark.parametrize(
    "name", ["profile", "prompt", "generation_route", "input_schema", "output_schema"]
)
async def test_evaluation_rejects_changed_generation_fingerprint(assets, evaluation_release, name):
    from qs_ai.application.evaluation.release import resolve_generation_assets

    changed = replace(
        evaluation_release,
        **{name: replace(getattr(evaluation_release, name), fingerprint="sha256:" + "0" * 64)},
    )
    with pytest.raises(ManifestUnavailable, match="does not match"):
        await resolve_generation_assets(changed, *assets[0])


async def test_evaluation_does_not_accept_asset_short_schema_version(assets, evaluation_release):
    from qs_ai.application.evaluation.release import resolve_generation_assets

    changed = replace(
        evaluation_release, input_schema=replace(evaluation_release.input_schema, version="v1")
    )
    with pytest.raises(ManifestUnavailable, match="input_schema"):
        await resolve_generation_assets(changed, *assets[0])


async def test_semantic_route_uses_its_own_revision_without_latest_fallback(
    assets, evaluation_release
):
    from qs_ai.application.evaluation.release import resolve_semantic_route

    routes = assets[0][2]
    route = routes.get.return_value
    # Independent identity does not mandate a different provider or model in original QS.
    release = replace(evaluation_release, semantic_route=evaluation_release.generation_route)
    assert await resolve_semantic_route(release, routes) == route
    routes.get.assert_awaited_with(route.route, route.revision)
    missing = replace(release, semantic_route=replace(release.semantic_route, version="v999"))
    with pytest.raises(ManifestUnavailable, match="semantic route"):
        await resolve_semantic_route(missing, routes)
    routes.get.assert_awaited_with(route.route, "v999")


@pytest.mark.parametrize("missing", [True, False])
async def test_semantic_route_rejects_missing_or_mismatched_fingerprint(
    assets, evaluation_release, missing
):
    from qs_ai.application.evaluation.release import resolve_semantic_route

    routes = assets[0][2]
    release = replace(evaluation_release, semantic_route=evaluation_release.generation_route)
    if missing:
        routes.get.return_value = None
    else:
        release = replace(
            release,
            semantic_route=replace(release.semantic_route, fingerprint="sha256:" + "0" * 64),
        )
    with pytest.raises(ManifestUnavailable, match="semantic route"):
        await resolve_semantic_route(release, routes)
