"""Plan a future Profile over first immutable assets without inserting anything."""

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qs_ai.application.governance.profile_registration import RegisterProfile
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.prompt_freeze import freeze_asset
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.persistence.mysql import profile_registrations as profiles
from tests.test_generation_manifest import assets as assets
from tests.test_native_prompts import draft as draft


@pytest.fixture
async def planned(assets, draft, monkeypatch):
    stores, _ = assets
    source = stores[0].get.return_value
    prompt = freeze_asset(draft, "a" * 64)
    original = stores[2].get.return_value
    from qs_ai.application.interpretation.route_assets import executable_route

    changed = replace(executable_route(original), revision="planned-route")
    raw = changed.definition_json()
    route = RouteAsset(changed.route, changed.revision, changed.fingerprint(), raw)
    definition = json.loads(source.definition_json)
    definition["version"] = "planned-profile"
    definition["generation_policy"].update(
        prompt_template_id=prompt.template_id,
        prompt_version=prompt.version,
        provider_route=route.route,
    )
    command = RegisterProfile(
        uuid4(),
        profiles.reference(source),
        json.dumps(definition),
        AssetReference(
            prompt.template_id, prompt.version, prompt.fingerprint, prompt.package_sha256
        ),
        AssetReference(
            route.route, route.revision, route.fingerprint, hashlib.sha256(raw.encode()).hexdigest()
        ),
        "只读规划未来资产",
    )
    registry = {(type(source), source.profile_id, source.version): source}
    fallback = []

    class Reader:
        def __init__(self, db, table, asset_type):
            self.asset_type = asset_type

        async def get(self, identity, version):
            fallback.append((self.asset_type, identity, version))
            if self.asset_type.__name__ == "SchemaAsset":
                return await stores[3].get(identity, version)
            return registry.get((self.asset_type, identity, version))

    monkeypatch.setattr(profiles, "AssetSnapshotReader", Reader)
    prior = AsyncMock(return_value=None)
    monkeypatch.setattr(profiles, "load_receipt", prior)
    scope, at = DraftScope(1, 42), draft.saved_at
    db = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    return SimpleNamespace(**locals())


async def test_future_assets_are_validated_without_target_rows_or_new_business_io(planned):
    p = planned
    result = await profiles.plan_registration(
        p.db, p.scope, p.command, p.at, planned_prompt=p.prompt, planned_route=p.route
    )
    assert not result.existing
    assert result.receipt.manifest.prompt == p.command.prompt
    assert result.receipt.manifest.generation_route == p.command.generation_route
    assert result.receipt.registered_at == p.at
    assert result.profile.version == "planned-profile"
    # Only the target-absence check hits the underlying snapshot for the future Profile.
    assert (
        sum(
            identity == result.profile.profile_id and version == result.profile.version
            for _, identity, version in p.fallback
        )
        == 1
    )
    assert not any(
        version == p.prompt.version or version == p.route.revision for _, _, version in p.fallback
    )
    p.db.execute.assert_not_called()
    p.db.commit.assert_not_called()
    p.db.rollback.assert_not_called()
    assert p.prompt.package_json not in repr(result)


@pytest.mark.parametrize("damage", ["source", "target", "reference", "clock"])
async def test_invalid_future_plan_does_not_write_partial_assets(planned, damage):
    p = planned
    command, at = p.command, p.at
    if damage == "source":
        p.registry.clear()
    elif damage == "target":
        target = profiles.profile_from(command)
        p.registry[type(target), target.profile_id, target.version] = target
    elif damage == "reference":
        command = replace(command, prompt=replace(command.prompt, content_sha256="0" * 64))
    else:
        at = at.replace(tzinfo=None)
    with pytest.raises(ValueError):
        await profiles.plan_registration(
            p.db, p.scope, command, at, planned_prompt=p.prompt, planned_route=p.route
        )
    p.db.execute.assert_not_called()
    p.db.commit.assert_not_called()


async def test_existing_registration_path_still_requires_persisted_target(planned):
    p = planned
    with pytest.raises(ValueError):
        await profiles.manifest_for(p.db, p.command, profiles.profile_from(p.command))
    p.db.execute.assert_not_called()
