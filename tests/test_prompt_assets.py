import hashlib
import json
from dataclasses import replace

import pytest

from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.infrastructure.qs_server.prompts import load_prompt


def test_all_original_packages_preserve_source_identity_and_bytes():
    source, assets = baseline_assets()
    assert source == "qs-server:1b52081ea42c94dc5653ce91e8c7a1db9f85fc44"
    assert [a.version for a in assets] == ["v6"]
    for asset in assets:
        package = load_prompt(asset.template_id, asset.version)
        data = json.loads(asset.package_json)
        assert data["SystemMessage"] == package.system_message
        assert data["TaskTemplate"] == package.task_template
        assert data["Ref"]["GitBlobSHA"] == package.git_blob_sha
        assert asset.fingerprint == package.fingerprint
        assert asset.package_sha256 == hashlib.sha256(asset.package_json.encode()).hexdigest()
        assert asset.fingerprint != "sha256:" + asset.package_sha256
        for changes in (
            {"template_id": "other"},
            {"version": "v9"},
            {"fingerprint": "sha256:" + "0" * 64},
            {"package_json": asset.package_json + " "},
        ):
            with pytest.raises(ValueError):
                replace(asset, **changes)
