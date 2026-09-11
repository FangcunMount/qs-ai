"""Immutable Prompt packages. Source fingerprints and package checksums are distinct."""

import hashlib
import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PromptAsset:
    template_id: str
    version: str
    fingerprint: str
    package_sha256: str
    package_json: str

    def __post_init__(self) -> None:
        if not self.template_id.strip() or len(self.template_id) > 255:
            raise ValueError("Invalid Prompt identity")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.version):
            raise ValueError("Invalid Prompt version")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.fingerprint):
            raise ValueError("Invalid source Prompt fingerprint")
        if len(self.package_json.encode()) > 131072:
            raise ValueError("Prompt package exceeds limit")
        if hashlib.sha256(self.package_json.encode()).hexdigest() != self.package_sha256:
            raise ValueError("Prompt package checksum mismatch")
        data = json.loads(self.package_json)
        if not isinstance(data, dict) or not isinstance(data.get("Ref"), dict):
            raise ValueError("Invalid Prompt reference")
        ref = data["Ref"]
        if (ref.get("TemplateID"), ref.get("Version"), ref.get("Fingerprint")) != (
            self.template_id,
            self.version,
            self.fingerprint,
        ):
            raise ValueError("Prompt reference mismatch")
        if not isinstance(ref.get("GitBlobSHA"), str) or not re.fullmatch(
            r"[0-9a-f]{40}", ref["GitBlobSHA"]
        ):
            raise ValueError("Invalid source Git blob")
        for name in ("SystemMessage", "TaskTemplate", "DataPreamble"):
            if not isinstance(data.get(name), str) or not data[name].strip():
                raise ValueError("Invalid Prompt text")
        placeholders = data.get("AllowedPlaceholders")
        if not isinstance(placeholders, list) or any(not isinstance(x, str) for x in placeholders):
            raise ValueError("Invalid Prompt placeholders")
