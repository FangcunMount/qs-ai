from dataclasses import replace

import pytest

from qs_ai.bootstrap.import_schemas import baseline_assets


def test_original_schemas_are_imported_without_rewriting_bytes_or_identity():
    source, assets = baseline_assets()
    assert source == "qs-server:1b52081ea42c94dc5653ce91e8c7a1db9f85fc44"
    assert [a.schema_id for a in assets] == ["ai-explanation-input", "ai-explanation-output"]
    for asset in assets:
        with pytest.raises(ValueError):
            replace(asset, version="v2")
        with pytest.raises(ValueError):
            replace(asset, definition_json=asset.definition_json + " ")
