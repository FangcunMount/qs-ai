"""Immutable imported definitions; importing is not approval or activation."""

import hashlib
import json
import re
from dataclasses import dataclass


class AssetConflict(ValueError):
    pass


@dataclass(frozen=True)
class ProfileAsset:
    profile_id: str
    version: str
    fingerprint: str
    definition_json: str

    def __post_init__(self) -> None:
        if not self.profile_id.strip() or len(self.profile_id) > 255:
            raise ValueError("Invalid Profile identity")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.version):
            raise ValueError("Invalid Profile version")
        if len(self.definition_json.encode()) > 131072:
            raise ValueError("Profile definition exceeds limit")
        definition = json.loads(self.definition_json)
        if not isinstance(definition, dict) or (
            definition.get("profile_id"),
            definition.get("version"),
        ) != (self.profile_id, self.version):
            raise ValueError("Profile definition identity mismatch")
        if (
            self.fingerprint
            != "sha256:" + hashlib.sha256(self.definition_json.encode()).hexdigest()
        ):
            raise ValueError("Profile fingerprint mismatch")
