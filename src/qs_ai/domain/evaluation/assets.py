"""Exact immutable evaluation assets; ownership is separate from their content digest."""

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum

from qs_ai.domain.evaluation.identity import FrozenContractRef


class PolicyKind(StrEnum):
    EXECUTION = "execution"
    GATE = "gate"


def verify_content(reference: FrozenContractRef, content: str, limit: int) -> None:
    raw = content.encode()
    if (
        not 1 <= len(raw) <= limit
        or "sha256:" + hashlib.sha256(raw).hexdigest() != reference.fingerprint
    ):
        raise ValueError("Evaluation asset content mismatch")


@dataclass(frozen=True)
class PolicyAsset:
    kind: PolicyKind
    reference: FrozenContractRef
    definition_json: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, PolicyKind):
            raise ValueError("Invalid evaluation policy kind")
        verify_content(self.reference, self.definition_json, 131072)
        if not self.reference.matches_document(self.definition_json):
            raise ValueError("Evaluation policy identity mismatch")
        if not isinstance(json.loads(self.definition_json), dict):
            raise ValueError("Invalid evaluation policy document")


@dataclass(frozen=True)
class SemanticPromptAsset:
    reference: FrozenContractRef
    markdown: str
    organization_id: int = 0

    def __post_init__(self) -> None:
        verify_content(self.reference, self.markdown, 131072)
        if type(self.organization_id) is not int or self.organization_id < 0:
            raise ValueError("Invalid semantic asset owner")
