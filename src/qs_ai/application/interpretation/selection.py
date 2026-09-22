"""Select only from the trusted standard-report snapshot already bound to a session."""

import json
from dataclasses import asdict
from typing import Any

from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.governance.publication import ReleaseSelector
from qs_ai.domain.interpretation.model import EvidenceSet, RuleViolation, Session


def snapshot_selector(snapshot: dict[str, Any]) -> ReleaseSelector:
    """Finite scene dispatch; v2 validation is available before production admission.

    The session admission path below remains v1 until v2 execution readers and
    immutable assets are deployed. This helper never chooses a latest version.
    """
    if snapshot.get("schema_version") == "qs-report-snapshot/v2":
        from qs_ai.application.interpretation.mbti_input import decode_mbti_snapshot

        value = decode_mbti_snapshot(snapshot)
        return ReleaseSelector(
            "participant",
            value["model"]["kind"],
            value["runtime"]["decision_kind"],
            value["model"]["code"],
            value["model"]["version"],
        )
    if snapshot.get("schema_version") != "qs-report-snapshot/v1":
        raise ValueError("Unsupported report snapshot version")
    model, runtime = snapshot["model"], snapshot["runtime"]
    if (model["kind"], runtime["decision_kind"]) != ("scale", "score_range"):
        raise ValueError("Snapshot v1 only supports scale/score_range")
    return ReleaseSelector(
        "participant", model["kind"], runtime["decision_kind"], model["code"], model["version"]
    )


def report_selector(session: Session, evidence: EvidenceSet) -> ReleaseSelector:
    evidence.validate(session.testee_id, session.assessment_ids)
    if (
        not session.uses_qs_snapshot
        or evidence.session_id != session.id
        or session.evidence_set_id != evidence.id
        or evidence.fingerprint != fingerprint([asdict(item) for item in evidence.items])
        or len(evidence.items) != 1
    ):
        raise RuleViolation("evidence_binding_mismatch")
    item = evidence.items[0]
    if len(item.facts) != 1 or item.facts[0].ref != "standard_report":
        raise RuleViolation("report_fact_missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value = {}
        for key, entry in pairs:
            if key in value:
                raise ValueError("Duplicate report field")
            value[key] = entry
        return value

    try:
        snapshot = json.loads(item.facts[0].value, object_pairs_hook=unique)
        source, model = snapshot["source"], snapshot["model"]
        if (
            snapshot["schema_version"] != "qs-report-snapshot/v1"
            or source["report_type"] != "standard"
            or source["report_id"] != item.report_id
            or f"{source['content_schema_version']}:{source['outcome_id']}" != item.source_version
            or not model["code"]
            or not model["version"]
        ):
            raise ValueError("Invalid report source")
        return snapshot_selector(snapshot)
    except (ValueError, TypeError, KeyError):
        raise RuleViolation("report_selection_invalid") from None
