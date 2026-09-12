"""Native Prompt provenance; imported QS packages keep their original format."""

import hashlib
import json
import re
from typing import Any
from uuid import UUID

from qs_ai.domain.governance.manifest import AssetReference

FORMAT = "qs-ai-prompt/v1"
SYNTAX_VERSION = "qs-ai-prompt-syntax/v1"


def canonical(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def native_fingerprint(data: dict[str, Any]) -> str:
    body = {**data, "Ref": {k: v for k, v in data["Ref"].items() if k != "Fingerprint"}}
    return "sha256:" + hashlib.sha256((FORMAT + "\n" + canonical(body)).encode()).hexdigest()


def validate_native_origin(data: dict[str, Any], fingerprint: str) -> None:
    if (
        set(data)
        != {
            "Format",
            "Ref",
            "Origin",
            "SystemMessage",
            "TaskTemplate",
            "DataPreamble",
            "AllowedPlaceholders",
        }
        or data["Format"] != FORMAT
    ):
        raise ValueError("Unknown native Prompt format")
    if set(data["Ref"]) != {"TemplateID", "Version", "Fingerprint"}:
        raise ValueError("Native Prompt cannot claim a QS Git blob")
    origin = data["Origin"]
    if not isinstance(origin, dict) or set(origin) != {
        "kind",
        "organization_id",
        "draft_id",
        "revision",
        "snapshot_sha256",
        "source",
        "validator_version",
    }:
        raise ValueError("Incomplete native Prompt origin")
    if origin["kind"] != "draft_revision" or origin["validator_version"] != SYNTAX_VERSION:
        raise ValueError("Unknown native Prompt origin or validator")
    identity = UUID(origin["draft_id"])
    if identity.int == 0 or str(identity) != origin["draft_id"]:
        raise ValueError("Canonical draft identity required")
    if any(
        type(origin[k]) is not int or not 0 < origin[k] < 2**63
        for k in ("organization_id", "revision")
    ):
        raise ValueError("Invalid native Prompt revision")
    if not isinstance(origin["snapshot_sha256"], str) or not re.fullmatch(
        r"[0-9a-f]{64}", origin["snapshot_sha256"]
    ):
        raise ValueError("Invalid native Prompt source checksum")
    source = AssetReference(**origin["source"])
    if (source.identity, source.version) == (data["Ref"]["TemplateID"], data["Ref"]["Version"]):
        raise ValueError("Native Prompt must have a new target version")
    if native_fingerprint(data) != fingerprint:
        raise ValueError("Native Prompt fingerprint mismatch")
