import hashlib
import json
from dataclasses import replace

import pytest

from qs_ai.bootstrap.import_routes import baseline_assets
from qs_ai.infrastructure.qs_server.routes import load_route


def test_route_asset_preserves_original_fingerprint_and_rejects_secrets():
    _, assets = baseline_assets()
    asset = assets[0]
    route = load_route(asset.route, asset.revision)
    assert asset.definition_json == route.definition_json()
    assert asset.fingerprint == route.fingerprint()
    with pytest.raises(ValueError):
        replace(asset, revision="v9")
    for field in ("endpoint", "api_key"):
        data = json.loads(asset.definition_json)
        data[field] = "synthetic-only"
        raw = json.dumps(data)
        with pytest.raises(ValueError, match="external"):
            replace(
                asset,
                definition_json=raw,
                fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
            )
