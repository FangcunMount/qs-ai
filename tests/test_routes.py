import hashlib
import json
import shutil

import pytest

from qs_ai.infrastructure.qs_server.routes import load_route, route_directory


def test_baseline_round_trip_and_fail_closed_on_modified_package(tmp_path):
    route = load_route("balanced_text_v1", "v8")
    assert (route.model, route.max_output_tokens, route.reasoning_effort) == (
        "deepseek-v4-pro",
        12000,
        "none",
    )
    shutil.copytree(route_directory(), tmp_path, dirs_exist_ok=True)
    path = tmp_path / "balanced_text_v1-v8.json"
    envelope = json.loads(path.read_bytes())
    envelope["definition"]["max_output_tokens"] = 8000
    path.write_text(json.dumps(envelope))
    with pytest.raises(ValueError):
        load_route("balanced_text_v1", "v8", directory=tmp_path)
    # Updating the transport checksum cannot hide a source fingerprint mismatch.
    manifest = json.loads((tmp_path / "manifest.json").read_bytes())
    manifest["files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        load_route("balanced_text_v1", "v8", directory=tmp_path)


@pytest.mark.parametrize("identity", [("balanced_text_v1", "v9"), ("../v8", "v8")])
def test_unregistered_route_is_rejected(identity):
    with pytest.raises(ValueError):
        load_route(*identity)
