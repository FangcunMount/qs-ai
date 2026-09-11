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
