"""Versioned acceptance supplement; the original v2 assets and historical gates stay intact."""

import hashlib
import json
from datetime import datetime

from qs_ai.domain.evaluation.resolution import valid_id

RULE_VERSION = "candidate-completion/v1"
RULE_JSON = json.dumps(
    {
        "version": RULE_VERSION,
        "base_policy": "release-gates/v2",
        "g3": {"candidate_completion_rate": 1.0, "execution_rates": "observation_only"},
        "preserve": [
            "frozen_identity",
            "complete_inventory",
            "recovery_authorization",
            "execution_budgets",
            "unresolved_call_rejection",
            "candidate_quality",
            "human_accountability",
        ],
    },
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
)
RULE_FINGERPRINT = "sha256:" + hashlib.sha256(RULE_JSON.encode()).hexdigest()


def rule_document() -> dict:
    return {"version": RULE_VERSION, "definition_json": RULE_JSON, "fingerprint": RULE_FINGERPRINT}


def acceptance_version(creation: dict, progress: dict, version: int, at: datetime) -> str | None:
    """Old Runs retain old math unless created with, or explicitly adopted, this supplement."""
    created = creation.get("acceptance_rule")
    adopted = progress.get("acceptance_rule_adoption")
    if created is not None:
        if created != rule_document() or adopted is not None:
            raise ValueError("Frozen acceptance rule mismatch")
        return RULE_VERSION
    if adopted is None:
        return None
    if (
        not isinstance(adopted, dict)
        or set(adopted)
        != {
            "rule",
            "actor",
            "reason",
            "adopted_at",
            "source_version",
            "version",
            "release_fingerprint",
        }
        or adopted["rule"] != rule_document()
        or adopted["release_fingerprint"] != creation["release_fingerprint"]
        or not valid_id(adopted["actor"])
        or not isinstance(adopted["reason"], str)
        or not adopted["reason"].strip()
        or len(adopted["reason"].encode()) > 1000
        or any(c in adopted["reason"] for c in "<>")
        or type(adopted["source_version"]) is not int
        or type(adopted["version"]) is not int
        or not 1 <= adopted["source_version"] < adopted["version"] <= version
        or adopted["version"] != adopted["source_version"] + 1
    ):
        raise ValueError("Invalid acceptance rule adoption audit")
    adopted_at = datetime.fromisoformat(adopted["adopted_at"])
    closures = [
        t for t in progress["transitions"] if t["cause_code"] == "candidate_evidence_complete"
    ]
    if (
        adopted_at.utcoffset() is None
        or len(closures) != 1
        or not datetime.fromisoformat(closures[0]["at"]) <= adopted_at <= at
    ):
        raise ValueError("Acceptance adoption outside completed evidence interval")
    return RULE_VERSION
