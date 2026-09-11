"""Immutable imported definitions; importing is not approval or activation."""

import hashlib
import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SchemaAsset:
    schema_id: str
    version: str
    fingerprint: str
    definition_json: str

    def __post_init__(self) -> None:
        if not self.schema_id.strip() or len(self.schema_id) > 255:
            raise ValueError("Invalid Schema identity")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.version):
            raise ValueError("Invalid Schema version")
        if len(self.definition_json.encode()) > 131072:
            raise ValueError("Schema definition exceeds limit")
        definition = json.loads(self.definition_json)
        if not isinstance(definition, dict):
            raise ValueError("Invalid schema document")
        properties = definition.get("properties")
        if not isinstance(properties, dict) or properties.get("schema_version") != {
            "const": f"{self.schema_id}/{self.version}"
        }:
            raise ValueError("Schema identity mismatch")
        if (
            self.fingerprint
            != "sha256:" + hashlib.sha256(self.definition_json.encode()).hexdigest()
        ):
            raise ValueError("Schema fingerprint mismatch")
