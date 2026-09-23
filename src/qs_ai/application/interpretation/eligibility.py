"""Read-only capability checks of a QS-authorized report, never an admission grant."""

import json
from dataclasses import asdict, dataclass
from typing import Protocol

from qs_ai.application.interpretation.input_values import _object
from qs_ai.application.interpretation.selection import report_selector
from qs_ai.application.interpretation.service import (
    external_id,
    fingerprint,
    published_workflow_version,
    validate_session_input,
)
from qs_ai.domain.governance.publication import ReleaseSelector
from qs_ai.domain.governance.scenes import MBTI_MODEL, MBTI_VERSION
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceItem,
    EvidenceSet,
    RuleViolation,
    Session,
)


@dataclass(frozen=True)
class Eligibility:
    status: str
    reason_code: str = ""


class EligibilityReader(Protocol):
    async def check(
        self,
        actor: Actor,
        testee_id: str,
        assessment_ids: tuple[str, ...],
        items: tuple[EvidenceItem, ...],
    ) -> Eligibility: ...


def eligibility_source(
    actor: Actor,
    testee_id: str,
    assessment_ids: tuple[str, ...],
    items: tuple[EvidenceItem, ...],
) -> tuple[Session, EvidenceSet, ReleaseSelector] | Eligibility:
    """Transient validation values only; these identities must never be persisted."""
    if not external_id(actor.org_id) or not actor.subject_id or len(actor.subject_id) > 128:
        raise RuleViolation("invalid_session_input")
    validate_session_input(testee_id, assessment_ids, "能力预检")
    if len(assessment_ids) != 1:
        return Eligibility("unavailable", "unsupported_scene")
    evidence = EvidenceSet("eligibility-evidence", "eligibility", "", items)
    try:
        evidence.validate(testee_id, assessment_ids)
    except RuleViolation as error:
        reason = (
            "source_conflict" if error.code == "evidence_subject_mismatch" else "source_incomplete"
        )
        return Eligibility("unavailable", reason)
    item = items[0]
    if len(item.facts) != 1 or item.facts[0].ref != "standard_report":
        return Eligibility("unavailable", "source_incomplete")
    try:
        snapshot = json.loads(item.facts[0].value, object_pairs_hook=_object)
        model, runtime, source = snapshot["model"], snapshot["runtime"], snapshot["source"]
        kind = model["kind"], runtime["decision_kind"]
        if kind not in {("scale", "score_range"), ("typology", "pole_composition")}:
            return Eligibility("unavailable", "unsupported_scene")
        if kind == ("typology", "pole_composition"):
            if model["code"] != MBTI_MODEL or model["version"] != MBTI_VERSION:
                return Eligibility("unavailable", "unsupported_model_version")
            if snapshot["schema_version"] != "qs-report-snapshot/v2":
                return Eligibility("unavailable", "source_incomplete")
        if (
            source["report_id"] != item.report_id
            or f"{source['content_schema_version']}:{source['outcome_id']}" != item.source_version
        ):
            return Eligibility("unavailable", "source_conflict")
        evidence = EvidenceSet(evidence.id, evidence.session_id, fingerprint([asdict(item)]), items)
        session = Session(
            evidence.session_id,
            actor,
            testee_id,
            assessment_ids,
            "能力预检",
            evidence_set_id=evidence.id,
            workflow_version=published_workflow_version(items),
        )
        return session, evidence, report_selector(session, evidence)
    except (ValueError, TypeError, KeyError, RuleViolation):
        return Eligibility("unavailable", "source_incomplete")
