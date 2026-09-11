import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest

from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


def test_semantic_assets_preserve_original_blocks_and_schema():
    assets = load_semantic_assets()
    assert "```text\n" + assets.system_message + "\n```" in assets.prompt_markdown
    assert "```text\n" + assets.task_message + "\n```" in assets.prompt_markdown
    assert (
        assets.data_preamble + "\n\n{{semantic_evaluation_payload_json}}" in assets.prompt_markdown
    )
    assert assets.output_schema.id == "ai-explanation-semantic-output"
    assert assets.output_schema.version == "ai-explanation-semantic-evaluation-output/v1"
    assert (
        hashlib.sha256(assets.output_schema_json.encode()).hexdigest()
        in assets.output_schema.fingerprint
    )


def test_semantic_instructions_cannot_change_with_recomputed_manifest(tmp_path):
    shutil.copytree(evaluation_directory(), tmp_path, dirs_exist_ok=True)
    path = tmp_path / "ai-explanation-semantic-evaluator-prompt-v2.md"
    path.write_bytes(path.read_bytes() + b"\n")
    manifest = json.loads((tmp_path / "manifest.json").read_bytes())
    manifest["files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="fingerprint"):
        load_semantic_assets(directory=tmp_path)


def test_semantic_assets_match_original_qs_source_bytes():
    source = os.getenv("QS_AI_PROMPT_SOURCE")
    if not source:
        pytest.skip("Requires original QS source")
    assets = load_semantic_assets()
    directory = Path(source) / "api/schema/interpretation"
    assert (
        assets.prompt_markdown.encode()
        == (directory / "ai-explanation-semantic-evaluator-prompt-v2.md").read_bytes()
    )
    assert (
        assets.output_schema_json.encode()
        == (directory / "ai-explanation-semantic-evaluation-output-v1.schema.json").read_bytes()
    )
