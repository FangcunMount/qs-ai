import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1] / "integrations/qs_server/evaluation"


def test_original_evaluation_resources_and_executable_policy_fingerprints():
    manifest = json.loads((ROOT / "manifest.json").read_bytes())
    assert len(manifest["files"]) == 12
    for filename, checksum in manifest["files"].items():
        assert hashlib.sha256((ROOT / filename).read_bytes()).hexdigest() == checksum
    policies = json.loads((ROOT / "policies.json").read_bytes())
    for kind, schema_name in (
        ("execution_policy", "ai-explanation-evaluation-execution-policy-v1.schema.json"),
        ("gate_policy", "ai-explanation-release-gate-policy-v1.schema.json"),
    ):
        asset = policies[kind]
        assert (
            "sha256:" + hashlib.sha256(asset["definition_json"].encode()).hexdigest()
            == (asset["fingerprint"])
        )
        definition = json.loads(asset["definition_json"])
        assert definition["version"] == "v2"
        schema = json.loads((ROOT / schema_name).read_bytes())
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(definition)
    for i in range(1, 7):
        suite = json.loads(
            (ROOT / f"ai-explanation-prompt-evaluation-cases-v{i}.json").read_bytes()
        )
        assert suite["suite_version"] == f"ai-explanation-prompt-evaluation-cases/v{i}"
        assert suite["prompt"]["version"] == f"v{i}"
        assert suite["status"] == "planned"
