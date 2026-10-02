"""Reference gates for three topics, using only the prepared frozen material."""

import json
import re
from typing import Any

from qs_ai.application.interpretation.input import InvalidInput, MBTIThematicInputPolicy
from qs_ai.application.interpretation.mbti_themes_input import validate_mbti_themes_projection
from qs_ai.application.interpretation.output_types import DeterministicOutput, InvalidOutput
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.domain.governance.mbti_references import InvalidMBTIReferences
from qs_ai.domain.governance.scenes import MBTI_THEMATIC_CONTRACT


def validate_mbti_themes_output(
    content: dict[str, Any], prepared: PreparedExplanation
) -> DeterministicOutput:
    policy = prepared.release.input_policy
    if (
        not isinstance(policy, MBTIThematicInputPolicy)
        or content.get("schema_version") != "ai-explanation-output/v2"
        or content.get("scene_contract_version") != MBTI_THEMATIC_CONTRACT
    ):
        raise InvalidOutput("output_contract_mismatch")
    try:
        payload = json.loads(prepared.assembled_input.provider_payload)
        validate_mbti_themes_projection(payload, policy)
    except (InvalidInput, TypeError, ValueError):
        raise InvalidOutput("input_reference_invalid") from None
    facts = payload["facts"]
    selected = policy.reference_material.select(facts["model_result"]["type_code"])
    allowed = {
        "dimension": {item["ref"] for item in facts["dimensions"]},
        "standard_suggestion": {item["ref"] for item in facts["standard_suggestions"]},
        "overall_result": {"overall_result"},
        "model_result": {"model_result"},
    }

    def check_evidence(item: dict[str, Any]) -> tuple[str, ...]:
        if any(ref["ref"] not in allowed[ref["kind"]] for ref in item["evidence_refs"]):
            raise InvalidOutput("unresolved_evidence")
        return tuple(ref["ref"] for ref in item["evidence_refs"])

    check_evidence(content["summary"])
    for section in content["sections"]:
        for item in (*section["insights"], *section["reflection_questions"], *section["actions"]):
            evidence = check_evidence(item)
            for ref in item["reference_refs"]:
                try:
                    selected.validate_reference(ref, section["topic"], evidence)
                except InvalidMBTIReferences:
                    raise InvalidOutput("unresolved_topic_reference") from None
    narrative = json.dumps(content, ensure_ascii=False)
    mentioned = re.findall(r"(?<![A-Za-z])[IE][SN][FT][JP](?![A-Za-z])", narrative, re.I)
    if any(code.upper() != selected.type_code for code in mentioned):
        raise InvalidOutput("mbti_type_conflict")
    return DeterministicOutput(
        json.dumps(content, ensure_ascii=False, separators=(",", ":")),
        "qs-ai-output-mbti-three-topic/v1",
    )
