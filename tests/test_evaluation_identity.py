import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import asdict, fields, replace
from pathlib import Path

import pytest

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef


def identity():
    return EvidenceReleaseIdentity(
        **{
            f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
            for f in fields(EvidenceReleaseIdentity)
        }
    )


def test_every_release_component_changes_fingerprint():
    original = identity()
    for field in fields(original):
        changed = replace(
            original, **{field.name: replace(getattr(original, field.name), version="v2")}
        )
        assert changed.fingerprint() != original.fingerprint()
    with pytest.raises(ValueError):
        replace(original, semantic_route=None)


def test_frozen_policy_identity_requires_original_documents():
    policies = json.loads(Path("integrations/qs_server/evaluation/policies.json").read_text())
    refs = {}
    for name, policy in policies.items():
        document = json.loads(policy["definition_json"])
        refs[name] = FrozenContractRef(
            document["policy_id"], document["version"], policy["fingerprint"]
        )
    release = replace(identity(), **refs)
    execution = policies["execution_policy"]["definition_json"]
    gate = policies["gate_policy"]["definition_json"]
    release.validate_frozen_policies(execution, gate)
    with pytest.raises(ValueError):
        release.validate_frozen_policies(execution + " ", gate)
    with pytest.raises(ValueError):
        release.validate_frozen_policies(gate, execution)
    mismatched = replace(release, execution_policy=replace(refs["execution_policy"], version="v3"))
    with pytest.raises(ValueError):
        mismatched.validate_frozen_policies(execution, gate)
    assert hashlib.sha256(execution.encode()).hexdigest() in refs["execution_policy"].fingerprint


def test_release_fingerprint_matches_original_go():
    source = os.getenv("QS_AI_PROMPT_SOURCE")
    if not source or not shutil.which("go"):
        pytest.skip("Requires original QS source and Go")
    original = identity()
    # Go fields have no JSON tags on the containing identity.
    names = {
        "".join(part.capitalize() for part in f.name.split("_")): asdict(getattr(original, f.name))
        for f in fields(original)
    }
    program = """package main
import("encoding/json";"os"; evaluation "QS_EVALUATION")
func main(){
 var identity evaluation.EvidenceReleaseIdentity
 if err:=json.NewDecoder(os.Stdin).Decode(&identity);err!=nil{panic(err)}
 fingerprint,err:=identity.ExpectedFingerprint();if err!=nil{panic(err)}
 json.NewEncoder(os.Stdout).Encode(fingerprint)
}
"""
    with tempfile.TemporaryDirectory(
        prefix="qs_ai_identity_", dir=Path(source) / "scripts"
    ) as name:
        path = Path(name) / "main.go"
        path.write_text(program)
        result = subprocess.run(
            ["go", "run", str(path)],
            cwd=source,
            input=json.dumps(names),
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )
    assert json.loads(result.stdout) == original.fingerprint()
