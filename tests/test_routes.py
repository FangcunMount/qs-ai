import hashlib
import json
import shutil

import pytest

from qs_ai.infrastructure.qs_server.routes import load_route, route_directory


@pytest.mark.parametrize(
    "identity,tokens,effort",
    [("balanced_text_v1", 12000, "none"), ("semantic_judge_v1", 8000, "low")],
)
def test_baseline_round_trip_and_fail_closed_on_modified_package(
    tmp_path, identity, tokens, effort
):
    revision = "v8" if identity == "balanced_text_v1" else "v5"
    route = load_route(identity, revision)
    assert (route.model, route.max_output_tokens, route.reasoning_effort) == (
        "deepseek-v4-pro",
        tokens,
        effort,
    )
    shutil.copytree(route_directory(), tmp_path, dirs_exist_ok=True)
    path = tmp_path / f"{identity}-{revision}.json"
    envelope = json.loads(path.read_bytes())
    envelope["definition"]["max_output_tokens"] = 1
    path.write_text(json.dumps(envelope))
    with pytest.raises(ValueError):
        load_route(identity, revision, directory=tmp_path)
    # Updating the transport checksum cannot hide a source fingerprint mismatch.
    manifest = json.loads((tmp_path / "manifest.json").read_bytes())
    manifest["files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        load_route(identity, revision, directory=tmp_path)


@pytest.mark.parametrize("identity", [("balanced_text_v1", "v9"), ("../v8", "v8")])
def test_unregistered_route_is_rejected(identity):
    with pytest.raises(ValueError):
        load_route(*identity)
