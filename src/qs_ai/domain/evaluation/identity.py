"""Complete QS evaluation identity; references alone do not establish approval."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, fields


@dataclass(frozen=True)
class FrozenContractRef:
    id: str
    version: str
    fingerprint: str

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.id):
            raise ValueError("Invalid frozen contract id")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.version):
            raise ValueError("Invalid frozen contract version")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.fingerprint):
            raise ValueError("Invalid frozen contract fingerprint")

    def matches_document(self, definition_json: str) -> bool:
        document = json.loads(definition_json)
        return (
            document.get("policy_id") == self.id
            and document.get("version") == self.version
            and "sha256:" + hashlib.sha256(definition_json.encode()).hexdigest() == self.fingerprint
        )


@dataclass(frozen=True)
class EvidenceReleaseIdentity:
    suite: FrozenContractRef
    prompt: FrozenContractRef
    profile: FrozenContractRef
    input_schema: FrozenContractRef
    output_schema: FrozenContractRef
    generation_route: FrozenContractRef
    semantic_prompt: FrozenContractRef
    semantic_output_schema: FrozenContractRef
    semantic_route: FrozenContractRef
    execution_policy: FrozenContractRef
    gate_policy: FrozenContractRef

    def __post_init__(self) -> None:
        if any(not isinstance(getattr(self, f.name), FrozenContractRef) for f in fields(self)):
            raise ValueError("All eleven frozen contract references are required")

    def fingerprint(self) -> str:
        # Go sorts the outer map keys but preserves nested struct field order.
        raw = json.dumps(
            {
                f.name: asdict(getattr(self, f.name))
                for f in sorted(fields(self), key=lambda f: f.name)
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()

    def validate_frozen_policies(self, execution_json: str, gate_json: str) -> None:
        if not self.execution_policy.matches_document(
            execution_json
        ) or not self.gate_policy.matches_document(gate_json):
            raise ValueError("Frozen policy documents do not match release identity")
