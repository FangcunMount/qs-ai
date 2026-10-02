"""Three-topic local gates and same durable artifact pipeline; synthetic examples."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from qs_ai.application.execution.artifact import build_artifact
from qs_ai.application.execution.generation import GeneratedExplanation
from qs_ai.application.interpretation.output import InvalidOutput, validate_output
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.qs_server.deepseek_request import build_request
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from tests.test_mbti_runtime import mbti_case
from tests.test_mbti_themes_input import thematic_case


def output():
    value = json.loads(
        (
            Path(__file__).parent / "fixtures/personality_contract/three-topic-output.json"
        ).read_text()
    )
    for section in value["sections"]:
        for item in (*section["insights"], *section["reflection_questions"], *section["actions"]):
            item["reference_refs"] = [f"reference:{section['topic']}.ei.i"]
    value["limitations"] = [
        "本次结果仅供自我理解参考，不构成诊断，不能作为确定性结论。",
        "通用参考需结合实际体验核对，职业选择还需考虑兴趣、技能与价值观。",
    ]
    return value


def validate(value, request=None):
    request = request or thematic_case()[2]
    return validate_output(
        json.dumps(value, ensure_ascii=False),
        request.prepared,
        QSOutputParser.from_schema(request.schema),
    )


def test_model_result_can_support_single_axis_reference_in_new_scene_only():
    value = output()
    value["sections"][0]["insights"][0]["evidence_refs"] = [
        {"kind": "model_result", "ref": "model_result"}
    ]
    result = validate(value)
    assert result.validator_version == "qs-ai-output-mbti-three-topic/v1"


@pytest.mark.parametrize(
    "refs,error",
    [
        (["reference:missing"], "unresolved_topic_reference"),
        (["reference:career.ei.i"], "unresolved_topic_reference"),
        (["reference:personality.ei.e"], "unresolved_topic_reference"),
    ],
)
def test_missing_wrong_topic_and_unselected_pole_references_are_rejected(refs, error):
    value = output()
    value["sections"][0]["insights"][0]["reference_refs"] = refs
    with pytest.raises(InvalidOutput, match=error):
        validate(value)


def test_reference_requires_the_related_axis_or_type_fact():
    value = output()
    value["sections"][0]["insights"][0]["evidence_refs"] = [
        {"kind": "dimension", "ref": "dimension:JP"}
    ]
    with pytest.raises(InvalidOutput, match="unresolved_topic_reference"):
        validate(value)


def test_missing_standard_suggestion_is_not_manufactured():
    value = output()
    value["sections"][0]["insights"][0]["evidence_refs"] = [
        {"kind": "standard_suggestion", "ref": "suggestion:missing"}
    ]
    with pytest.raises(InvalidOutput, match="unresolved_evidence"):
        validate(value)


def test_report_fact_cannot_carry_unreviewed_reference_as_measured_fact():
    value = output()
    value["sections"][0]["insights"][0]["basis"] = "report_fact"
    with pytest.raises(InvalidOutput, match="output_schema_invalid"):
        validate(value)


def test_mutated_frozen_material_does_not_pass_output_gates():
    _, _, request = thematic_case()
    payload = json.loads(request.prepared.assembled_input.provider_payload)
    payload["reference_material"]["entries"][0]["content"] = "篡改来源正文"
    request = replace(
        request,
        prepared=replace(
            request.prepared,
            assembled_input=replace(
                request.prepared.assembled_input, provider_payload=json.dumps(payload)
            ),
        ),
    )
    with pytest.raises(InvalidOutput, match="input_reference_invalid"):
        validate(output(), request)


def test_old_scene_does_not_accept_new_output_even_with_new_parser():
    _, _, old = mbti_case()
    with pytest.raises(InvalidOutput, match="output_contract_mismatch"):
        validate(output(), replace(old, schema=thematic_case()[2].schema))


def test_other_type_is_still_rejected():
    value = output()
    value["sections"][1]["insights"][0]["content"] = "本次结果为 INTJ。"
    with pytest.raises(InvalidOutput, match="mbti_type_conflict"):
        validate(value)


def test_deepseek_projection_keeps_data_and_all_section_fields_without_changing_full_schema():
    _, _, request = thematic_case()
    original = json.dumps(request.schema)
    provider_request = build_request(request.prepared, request.route, request.schema)
    remote_schema = provider_request["text"]["format"]["schema"]
    section = remote_schema["properties"]["sections"]["items"]
    assert section["properties"]["topic"]["enum"] == ["personality", "career", "relationships"]
    assert set(section["required"]) == {"topic", "insights", "reflection_questions", "actions"}
    assert (
        len(
            section["properties"]["insights"]["items"]["properties"]["evidence_refs"]["items"][
                "anyOf"
            ]
        )
        == 4
    )
    assert "prefixItems" not in json.dumps(remote_schema)
    assert json.dumps(request.schema) == original
    assert provider_request["max_output_tokens"] == request.route.max_output_tokens
    assert request.prepared.messages.data_json in provider_request["input"][1]["content"][0]["text"]
    # Provider's subset cannot replace the full local topic-order contract.
    value = output()
    value["sections"].reverse()
    with pytest.raises(InvalidOutput, match="output_schema_invalid"):
        validate(value, request)


def test_artifact_replay_preserves_new_content_and_original_reference_binding():
    claim, evidence, request = thematic_case()
    raw = json.dumps(output(), ensure_ascii=False)
    response = ModelResponse(
        "invocation", "receipt", request.route.model, raw, raw, "none", 1, 2, 3
    )
    generated = GeneratedExplanation(request, response)
    parser = QSOutputParser.from_schema(request.schema)
    artifact = build_artifact(claim, evidence, generated, parser)
    codec = JSONModelCallCodec()
    restored = replace(generated, request=codec.decode_request(codec.encode_request(request)))
    assert build_artifact(claim, evidence, restored, parser) == artifact
    assert artifact.output_validator_version == "qs-ai-output-mbti-three-topic/v1"
    assert json.loads(artifact.content_json)["schema_version"] == "ai-explanation-output/v2"
    assert artifact.report_id == evidence.items[0].report_id
