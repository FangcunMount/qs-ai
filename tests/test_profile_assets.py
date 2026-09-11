import json
from dataclasses import replace

import pytest

from qs_ai.bootstrap.import_profiles import baseline_assets
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile


def test_baseline_assets_preserve_original_profile_fingerprint_and_semantics():
    source, assets = baseline_assets()
    assert source.startswith("qs-server:")
    assert assets
    for asset in assets:
        release = decode_published_profile(
            {
                "definition": json.loads(asset.definition_json),
                "fingerprint": asset.fingerprint,
                "status": "published",
            }
        )
        assert release.definition_json == asset.definition_json
        assert release.input_policy.profile_fingerprint == asset.fingerprint
        with pytest.raises(ValueError):
            replace(asset, profile_id="different-profile")
        with pytest.raises(ValueError):
            replace(asset, definition_json=asset.definition_json + " ")
