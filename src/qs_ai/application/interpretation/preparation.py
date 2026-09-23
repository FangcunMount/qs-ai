"""Bind a frozen session's evidence to the report used for input assembly."""

import json
from dataclasses import asdict, dataclass

from qs_ai.application.interpretation.input import (
    AssembledInput,
    InputPolicy,
    MBTIInputPolicy,
    assemble_input,
)
from qs_ai.application.interpretation.prompts import PromptMessages, PromptPackage, render_prompt
from qs_ai.application.interpretation.release import ExplanationRelease
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.interpretation.model import EvidenceSet, RuleViolation, Session


@dataclass(frozen=True)
class PreparedExplanation:
    assembled_input: AssembledInput
    messages: PromptMessages
    release: ExplanationRelease
    prompt_fingerprint: str


def prepare_explanation(
    session: Session,
    evidence: EvidenceSet,
    release: ExplanationRelease,
    package: PromptPackage,
    *,
    locale: str = "zh-CN",
    focus_areas: tuple[str, ...] = (),
) -> PreparedExplanation:
    assembled = prepare_report_input(
        session, evidence, release.input_policy, locale=locale, focus_areas=focus_areas
    )
    messages = render_prompt(package, release.render_policy, assembled.provider_payload)
    return PreparedExplanation(assembled, messages, release, package.fingerprint)


def prepare_report_input(
    session: Session,
    evidence: EvidenceSet,
    policy: InputPolicy,
    *,
    locale: str = "zh-CN",
    focus_areas: tuple[str, ...] = (),
) -> AssembledInput:
    # This validates binding and integrity, not current access. The execution
    # use case must still check QS authorization before generation and acceptance.
    evidence.validate(session.testee_id, session.assessment_ids)
    if (
        not session.uses_qs_snapshot
        or evidence.session_id != session.id
        or session.evidence_set_id != evidence.id
        or evidence.fingerprint != fingerprint([asdict(item) for item in evidence.items])
    ):
        raise RuleViolation("evidence_binding_mismatch")
    if len(evidence.items) != 1:
        raise RuleViolation("single_report_required")
    item = evidence.items[0]
    if len(item.facts) != 1 or item.facts[0].ref != "standard_report":
        raise RuleViolation("report_fact_missing")
    if (session.workflow_version == "qs-published-snapshot-v2") != isinstance(
        policy, MBTIInputPolicy
    ):
        raise RuleViolation("input_workflow_mismatch")
    assembled = assemble_input(
        item.facts[0].value,
        policy,
        locale=locale,
        focus_areas=focus_areas,
    )
    source = json.loads(assembled.canonical_json)["source"]
    if (
        source["report_id"] != item.report_id
        or f"{source['content_schema_version']}:{source['outcome_id']}" != item.source_version
    ):
        raise RuleViolation("report_source_mismatch")
    return assembled
