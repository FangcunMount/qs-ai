"""Frozen generation assets. This identity is not an evaluation approval."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AssetReference:
    identity: str
    version: str
    fingerprint: str
    content_sha256: str

    def __post_init__(self) -> None:
        if not self.identity.strip() or len(self.identity) > 255:
            raise ValueError("Invalid asset identity")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.version):
            raise ValueError("Invalid asset version")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.fingerprint):
            raise ValueError("Invalid source fingerprint")
        if not re.fullmatch(r"[0-9a-f]{64}", self.content_sha256):
            raise ValueError("Invalid asset checksum")


@dataclass(frozen=True)
class GenerationManifest:
    profile: AssetReference
    prompt: AssetReference
    generation_route: AssetReference
    input_schema: AssetReference
    output_schema: AssetReference

    def canonical_json(self) -> str:
        return json.dumps(
            {"schema_version": "qs-ai-generation-manifest/v1", **asdict(self)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(self.canonical_json().encode()).hexdigest()
