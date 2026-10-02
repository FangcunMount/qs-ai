"""Three-topic shape contract only; no source-support, quality or rollout claims."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.output import InvalidOutput
from qs_ai.infrastructure.qs_server.output import QSOutputParser

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "integrations" / "qs_server" / "schemas"
FIXTURE = ROOT / "tests" / "fixtures" / "personality_contract" / "three-topic-output.json"


@pytest.fixture
def schema():
    return json.loads((SCHEMAS / "ai-explanation-output-v2.schema.json").read_text())


@pytest.fixture
def candidate():
    # Reference IDs and prose are synthetic contract data, never approved content.
    return json.loads(FIXTURE.read_text())


def test_schema_is_valid_and_fixture_has_three_topics(schema, candidate):
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(candidate)
    assert [item["topic"] for item in candidate["sections"]] == [
        "personality",
        "career",
        "relationships",
    ]


def test_versioned_parser_preserves_basis_and_refs(schema, candidate):
    parser = QSOutputParser.from_schema(schema)
    parsed = parser.parse(json.dumps(candidate, ensure_ascii=False))
    assert parsed == candidate


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong_version",
        "wrong_scene",
        "missing_topic",
        "duplicate_topic",
        "wrong_topic_order",
        "extra_topic",
        "missing_insights",
        "no_questions",
        "no_actions",
        "unknown_basis",
        "reference_as_report_fact",
        "general_without_reference",
        "exploration_without_reference",
        "insight_without_report",
        "duplicate_report_ref",
        "invalid_axis",
        "wrong_model_ref",
        "unknown_reference_format",
        "model_supplied_source_url",
        "question_as_fact",
        "action_as_fact",
        "summary_as_reference",
        "summary_missing_evidence",
        "blank_content",
        "html_content",
        "oversized_content",
        "empty_limitations",
    ],
)
def test_malformed_or_misattributed_content_rejected(schema, candidate, mutation):
    value = deepcopy(candidate)
    section = value["sections"][0]
    insight = section["insights"][0]
    if mutation == "wrong_version":
        value["schema_version"] = "ai-explanation-output/v1"
    elif mutation == "wrong_scene":
        value["scene_contract_version"] = "mbti-single-assessment/v1"
    elif mutation == "missing_topic":
        value["sections"].pop()
    elif mutation == "duplicate_topic":
        value["sections"][1]["topic"] = "personality"
    elif mutation == "wrong_topic_order":
        value["sections"].reverse()
    elif mutation == "extra_topic":
        value["sections"].append(deepcopy(section))
    elif mutation == "missing_insights":
        section["insights"] = []
    elif mutation == "no_questions":
        section["reflection_questions"] = []
    elif mutation == "no_actions":
        section["actions"] = []
    elif mutation == "unknown_basis":
        insight["basis"] = "individual_personality_fact"
    elif mutation == "reference_as_report_fact":
        insight["basis"] = "report_fact"
    elif mutation == "general_without_reference":
        insight["reference_refs"] = []
    elif mutation == "exploration_without_reference":
        section["insights"][1]["reference_refs"] = []
    elif mutation == "insight_without_report":
        insight["evidence_refs"] = []
    elif mutation == "duplicate_report_ref":
        insight["evidence_refs"].append(deepcopy(insight["evidence_refs"][0]))
    elif mutation == "invalid_axis":
        insight["evidence_refs"][1]["ref"] = "dimension:IQ"
    elif mutation == "wrong_model_ref":
        insight["evidence_refs"][0]["ref"] = "dimension:EI"
    elif mutation == "unknown_reference_format":
        insight["reference_refs"] = ["https://example.invalid/unreviewed"]
    elif mutation == "model_supplied_source_url":
        insight["source_url"] = "https://example.invalid/unreviewed"
    elif mutation == "question_as_fact":
        section["reflection_questions"][0]["basis"] = "report_fact"
    elif mutation == "action_as_fact":
        section["actions"][0]["basis"] = "report_fact"
    elif mutation == "summary_as_reference":
        value["summary"]["basis"] = "general_reference"
    elif mutation == "summary_missing_evidence":
        value["summary"]["evidence_refs"] = []
    elif mutation == "blank_content":
        insight["content"] = "   \n "
    elif mutation == "html_content":
        insight["content"] = "<b>未经审核</b>"
    elif mutation == "oversized_content":
        insight["content"] = "长" * 601
    elif mutation == "empty_limitations":
        value["limitations"] = []
    else:
        raise AssertionError(mutation)
    with pytest.raises(InvalidOutput, match="output_schema_invalid"):
        QSOutputParser.from_schema(schema).parse(json.dumps(value, ensure_ascii=False))


def test_report_fact_can_use_one_axis_without_relaxing_other_versions(schema, candidate):
    statement = candidate["sections"][0]["insights"][0]
    statement.update(
        basis="report_fact",
        content="本次 EI 轴结果为 I。",
        evidence_refs=[{"kind": "dimension", "ref": "dimension:EI"}],
        reference_refs=[],
    )
    Draft202012Validator(schema).validate(candidate)


def test_old_runtime_parser_does_not_silently_accept_new_contract(candidate):
    with pytest.raises(InvalidOutput, match="output_schema_invalid"):
        QSOutputParser().parse(json.dumps(candidate, ensure_ascii=False))


def test_new_contract_parser_rejects_duplicate_json_fields(schema, candidate):
    raw = json.dumps(candidate, ensure_ascii=False)
    raw = raw.replace('"summary":', '"summary": {}, "summary":', 1)
    # The existing parser deliberately normalizes JSON decode failures.
    with pytest.raises(InvalidOutput, match="invalid_json"):
        QSOutputParser.from_schema(schema).parse(raw)


def test_old_schema_is_not_modified_by_three_topic_contract():
    assert hashlib.sha256(
        (SCHEMAS / "ai-explanation-output-v1.schema.json").read_bytes()
    ).hexdigest() == ("103cc2efc7abef6722729b30d1591f6e19ed2164f37901f7f5d789982fbb2744")
