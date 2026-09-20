"""Historical policies are checked against the Run, never a later deployment's files."""

import hashlib
from dataclasses import asdict, fields, replace

import pytest

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.evaluation_frozen_policies import frozen_policies
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)


def creation():
    policy, gate = load_execution_policy(), load_gate_policy()
    refs = {
        f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
        for f in fields(EvidenceReleaseIdentity)
    }
    refs.update(
        execution_policy=FrozenContractRef(policy.policy_id, policy.version, policy.fingerprint),
        gate_policy=gate.reference,
    )
    release = EvidenceReleaseIdentity(**refs)
    return {
        "release": asdict(release),
        "release_fingerprint": release.fingerprint(),
        "execution_policy_json": policy.definition_json,
        "gate_policy_json": gate.definition_json,
    }


def test_recovery_uses_original_bytes_without_reading_current_files(monkeypatch):
    original = creation()
    expected = load_execution_policy()

    def unavailable(*args, **kwargs):
        raise AssertionError("No current file may participate in historical recovery")

    monkeypatch.setattr("pathlib.Path.read_bytes", unavailable)
    monkeypatch.setattr("pathlib.Path.read_text", unavailable)
    assert frozen_policies(original)[0] == expected


@pytest.mark.parametrize(
    "field", ["execution_policy_json", "gate_policy_json", "release_fingerprint"]
)
def test_changed_or_missing_frozen_content_fails(field):
    value = creation()
    broken = {**value, field: value[field] + " "}
    with pytest.raises(ValueError):
        frozen_policies(broken)
    del value[field]
    with pytest.raises(KeyError):
        frozen_policies(value)


def test_explicit_older_policy_does_not_need_current_version():
    value = creation()
    # An exact historical version is self-contained, including its original raw bytes.
    raw = value["execution_policy_json"].replace('"v2"', '"retained-version"')
    reference = replace(
        FrozenContractRef(**value["release"]["execution_policy"]),
        version="retained-version",
        fingerprint="sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
    )
    refs = {key: FrozenContractRef(**item) for key, item in value["release"].items()}
    refs["execution_policy"] = reference
    release = EvidenceReleaseIdentity(**refs)
    value.update(
        release=asdict(release),
        release_fingerprint=release.fingerprint(),
        execution_policy_json=raw,
    )
    assert frozen_policies(value)[0].version == "retained-version"
