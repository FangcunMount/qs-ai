"""Profile derivation policy, using a fake asset reader rather than a database."""

from uuid import uuid4

import pytest

from qs_ai.application.governance.profile_registration import RegisterProfile
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.persistence.mysql import profile_registrations as registrations
from qs_ai.infrastructure.qs_server.profiles import canonical_definition
from tests.test_mbti_contract import profile, sign
from tests.test_mbti_themes_input import thematic_profile


@pytest.mark.parametrize("change", ["inherit", "reference_material", "scene"])
async def test_editing_cannot_replace_reference_material_or_upgrade_scene(monkeypatch, change):
    entry = profile() if change == "scene" else thematic_profile()
    definition = entry["definition"]
    source = ProfileAsset(
        definition["profile_id"],
        definition["version"],
        entry["fingerprint"],
        canonical_definition(definition),
    )

    class Assets:
        def __init__(self, *args):
            pass

        async def get(self, *args):
            return source

    monkeypatch.setattr(registrations, "AssetSnapshotReader", Assets)
    if change == "scene":
        entry = thematic_profile()
    elif change == "reference_material":
        definition["reference_material"]["entries"][0]["content"] = "未确认的新参考正文"
    entry["definition"]["version"] = "derived-test"
    target = sign(entry)
    unrelated_asset = AssetReference("synthetic", "v1", "sha256:" + "a" * 64, "a" * 64)
    command = RegisterProfile(
        uuid4(),
        registrations.reference(source),
        canonical_definition(target["definition"]),
        unrelated_asset,
        unrelated_asset,
        "验证方案继承固定主题参考；不发布",
    )
    if change == "inherit":
        await registrations.validate_source(None, command)
    else:
        with pytest.raises(ValueError, match="source scene"):
            await registrations.validate_source(None, command)
