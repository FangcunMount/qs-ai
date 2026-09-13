import hashlib
import json
from dataclasses import replace

import pytest

from qs_ai.bootstrap.import_routes import baseline_assets
from qs_ai.infrastructure.qs_server.routes import load_route


def test_evaluation_import_matches_observed_production_parameters():
    from pathlib import Path

    evidence = json.loads(
        (
            Path(__file__).parents[1]
            / "docs/evidence/2026-09-13-m3-current-published-reconciliation.json"
        ).read_text()
    )["qs_runtime_policy"]["configuration"]
    source, assets = baseline_assets(include_evaluation=True)
    assert source == "qs-server:runtime-observed-2026-09-13"
    assert len(assets) == 2
    route = load_route("semantic_judge_v1", evidence["evaluation.route_revision"])
    assert assets[1].definition_json == route.definition_json()
    assert route.model == evidence["evaluation.model"]
    assert route.protocol == evidence["evaluation.provider_protocol"]
    assert route.structured_output_mode == evidence["evaluation.structured_output_mode"]
    assert route.reasoning_effort == evidence["evaluation.reasoning_effort"]
    assert route.max_output_tokens == evidence["evaluation.max_output_tokens"]
    assert (
        route.timeout_milliseconds == int(evidence["evaluation.timeout"].removesuffix("s")) * 1000
    )
    # Existing generation-only imports and their audit remain unchanged.
    assert len(baseline_assets()[1]) == 1


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
