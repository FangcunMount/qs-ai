"""Recover the original Run policies from verified frozen bytes, never current files."""

from typing import Any

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.policy import ExecutionPolicy
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    FrozenPolicyDocument,
    execution_policy,
)


def frozen_policies(creation: dict[str, Any]) -> tuple[ExecutionPolicy, FrozenPolicyDocument]:
    release = EvidenceReleaseIdentity(
        **{key: FrozenContractRef(**value) for key, value in creation["release"].items()}
    )
    if creation["release_fingerprint"] != release.fingerprint():
        raise ValueError("Frozen Run release fingerprint mismatch")
    execution_json, gate_json = creation["execution_policy_json"], creation["gate_policy_json"]
    release.validate_frozen_policies(execution_json, gate_json)
    return (
        execution_policy(FrozenPolicyDocument(release.execution_policy, execution_json)),
        FrozenPolicyDocument(release.gate_policy, gate_json),
    )
