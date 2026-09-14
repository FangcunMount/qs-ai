"""A curated runtime inventory must not masquerade as the full historical export."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "integrations/qs_server"


def test_retained_resource_bytes_and_source_manifests_are_exact():
    inventory = json.loads((ROOT / "retained-assets.json").read_text())
    assert inventory["schema"] == "qs-ai-retained-assets/v1"
    for directory, files in inventory["directories"].items():
        source = ROOT / directory
        actual = {p.name for p in source.iterdir() if p.is_file() and p.name != "README.md"}
        assert actual == set(files), directory
        for name, checksum in files.items():
            assert hashlib.sha256((source / name).read_bytes()).hexdigest() == checksum
        manifest = json.loads((source / "manifest.json").read_bytes())
        for name in set(files) & manifest.get("files", {}).keys():
            assert files[name] == manifest["files"][name]
    prompts = inventory["directories"]["prompts"]
    assert "v6.json" in prompts
    assert not {f"v{i}.json" for i in range(1, 6)} & prompts.keys()
    evaluation = inventory["directories"]["evaluation"]
    assert "ai-explanation-prompt-evaluation-cases-v6.json" in evaluation
    assert "qs-ai-published-input-cases-v1.json" in evaluation
    assert (
        not {f"ai-explanation-prompt-evaluation-cases-v{i}.json" for i in range(1, 6)}
        & evaluation.keys()
    )
    assert "ai-explanation-semantic-evaluator-prompt-v1.md" not in evaluation
