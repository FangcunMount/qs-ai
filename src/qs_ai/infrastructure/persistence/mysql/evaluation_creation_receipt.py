"""Project the original creation identity; never resolve today's assets or execute a Run."""

import json
import re
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from typing import Any

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef


def creation_receipt(row: Mapping[str, Any]) -> str:
    definition = json.loads(row["definition_json"])
    if (
        definition["schema_version"] != "qs-ai-evaluation-run-creation/v1"
        or definition["run_id"] != row["run_id"]
    ):
        raise ValueError("Creation identity does not match stored Run")
    audit = definition["audit"]
    actor, reason, created_at = (
        audit["requested_by"],
        audit["request_reason"],
        audit["created_at"],
    )
    if (
        type(audit["organization_id"]) is not int
        or audit["organization_id"] != row["organization_id"]
        or actor != row["requested_by"]
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", actor)
        or not isinstance(reason, str)
        or not reason.strip()
        or len(reason.encode()) > 1000
        or any(x in reason for x in "<>")
        or datetime.fromisoformat(created_at).utcoffset() is None
    ):
        raise ValueError("Creation audit does not match stored Run")
    release = EvidenceReleaseIdentity(
        **{key: FrozenContractRef(**ref) for key, ref in definition["release"].items()}
    )
    if release.fingerprint() != definition["release_fingerprint"]:
        raise ValueError("Creation release fingerprint does not match")
    return json.dumps(
        {
            "schema_version": "qs-ai-evaluation-creation-receipt/v1",
            "run_id": row["run_id"],
            "release": asdict(release),
            "release_fingerprint": release.fingerprint(),
            "requested_by": actor,
            "request_reason": reason,
            "created_at": created_at,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
