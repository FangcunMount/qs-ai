import hashlib
import json
from dataclasses import asdict, fields, replace
from pathlib import Path

import pytest

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from tests.legacy_go_baseline import legacy_go_result


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


def test_release_fingerprint_matches_retained_go_baseline():
    original = identity()
    # Go fields have no JSON tags on the containing identity.
    names = {
        "".join(part.capitalize() for part in f.name.split("_")): asdict(getattr(original, f.name))
        for f in fields(original)
    }
    result = legacy_go_result(names)
    assert json.loads(result.stdout) == original.fingerprint()
