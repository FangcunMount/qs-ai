"""Load the frozen original Go execution policy without reconstructing its fingerprint."""

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.policy import ExecutionPolicy


def evaluation_directory() -> Path:
    bundled = Path(__file__).resolve().parent / "evaluation_assets"
    if bundled.is_dir():
        return bundled
    return Path(__file__).resolve().parents[4] / "integrations" / "qs_server" / "evaluation"


@dataclass(frozen=True)
class FrozenPolicyDocument:
    reference: FrozenContractRef
    definition_json: str


def _load_document(
    kind: str, schema_name: str, expected_id: str, directory: Path | None
) -> FrozenPolicyDocument:
    directory = directory if directory is not None else evaluation_directory()
    manifest = json.loads((directory / "manifest.json").read_bytes())

    def verified(name: str) -> bytes:
        raw = (directory / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest["files"][name]:
            raise ValueError("Evaluation resource checksum mismatch")
        return raw

    envelope = json.loads(verified("policies.json"))[kind]
    raw = envelope["definition_json"]
    if "sha256:" + hashlib.sha256(raw.encode()).hexdigest() != envelope["fingerprint"]:
        raise ValueError("Policy source fingerprint mismatch")
    definition = json.loads(raw)
    schema = json.loads(verified(schema_name))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(definition)
    if (definition["policy_id"], definition["version"]) != (
        expected_id,
        "v2",
    ):
        raise ValueError("Unsupported policy version")
    return FrozenPolicyDocument(
        FrozenContractRef(definition["policy_id"], definition["version"], envelope["fingerprint"]),
        raw,
    )


def load_gate_policy(*, directory: Path | None = None) -> FrozenPolicyDocument:
    return _load_document(
        "gate_policy",
        "ai-explanation-release-gate-policy-v1.schema.json",
        "release-gates",
        directory,
    )


def load_execution_policy(*, directory: Path | None = None) -> ExecutionPolicy:
    document = _load_document(
        "execution_policy",
        "ai-explanation-evaluation-execution-policy-v1.schema.json",
        "release-evaluation-bounded-recovery",
        directory,
    )
    raw = document.definition_json
    definition = json.loads(raw)
    slot, generation, semantic, recovery = (
        definition["slot_policy"],
        definition["generation_budget"],
        definition["semantic_budget"],
        definition["recovery_policy"],
    )
    return ExecutionPolicy(
        definition["policy_id"],
        definition["version"],
        document.reference.fingerprint,
        raw,
        slot["required_generation_cases"],
        slot["required_candidates_per_case"],
        slot["required_preflight_cases"],
        generation["max_executions_per_slot"],
        generation["max_executions_per_run"],
        semantic["max_executions_per_candidate"],
        semantic["max_executions_per_run"],
        frozenset((x["stage"], x["code"]) for x in recovery["auto_retryable_stage_codes"]),
        frozenset((x["stage"], x["code"]) for x in recovery["manual_recovery_stage_codes"]),
        recovery["result_unknown_requires_manual_acknowledgement"],
        recovery["quality_failure_replacement_allowed"],
        recovery["semantic_failure_regenerates_candidate"],
    )
