"""A planner can resolve future immutable assets without writing or hiding fallback reads."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import PlannedAssetReader


@pytest.mark.parametrize("kind", ["profile", "prompt", "route", "schema"])
async def test_only_exact_planned_kind_identity_and_version_override_this_snapshot(kind):
    from qs_ai.bootstrap.import_profiles import baseline_assets as profiles
    from qs_ai.bootstrap.import_prompts import baseline_assets as prompts
    from qs_ai.bootstrap.import_routes import baseline_assets as routes
    from qs_ai.bootstrap.import_schemas import baseline_assets as schemas

    planned = {"profile": profiles, "prompt": prompts, "route": routes, "schema": schemas}[kind]()[
        1
    ][0]
    asset_type = type(planned)
    key = {
        "profile": ("profile_id", "version"),
        "prompt": ("template_id", "version"),
        "route": ("route", "revision"),
        "schema": ("schema_id", "version"),
    }[kind]
    identity, version = (getattr(planned, n) for n in key)
    retained = object()
    original = SimpleNamespace(asset_type=asset_type, get=AsyncMock(return_value=retained))
    reader = PlannedAssetReader(original, (planned,))
    original.get.assert_not_called()
    assert await reader.get(identity, version) is planned
    original.get.assert_not_called()
    assert await reader.get(identity, "other") is retained
    original.get.assert_awaited_once_with(identity, "other")


@pytest.mark.parametrize("damage", ["duplicate", "kind", "mutable"])
def test_ambiguous_or_mutable_planned_assets_are_rejected_before_reading(damage):
    from qs_ai.bootstrap.import_profiles import baseline_assets

    asset = baseline_assets()[1][0]
    original = SimpleNamespace(asset_type=ProfileAsset, get=AsyncMock())
    values = (
        (asset, asset) if damage == "duplicate" else (object(),) if damage == "kind" else [asset]
    )
    with pytest.raises(ValueError):
        PlannedAssetReader(original, values)
    original.get.assert_not_called()
