import json
from dataclasses import replace
from pathlib import Path

import pytest

from qs_ai.application.interpretation.input import assemble_input
from qs_ai.application.interpretation.output import InvalidOutput, validate_output
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.application.interpretation.safety import check_safety
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.legacy_go_baseline import legacy_go_result

A = "dimension:A%2F%E8%A7%82%E5%AF%9F"
B = "dimension:B"


@pytest.fixture
def prepared() -> PreparedExplanation:
    release = load_migrated_release("participant-scale-score-range-default", "v6")
    raw = (Path(__file__).parent / "fixtures" / "report_snapshot.json").read_text()
    assembled = assemble_input(raw, release.input_policy)
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    return PreparedExplanation(
        assembled,
        render_prompt(package, release.render_policy, assembled.provider_payload),
        release,
        package.fingerprint,
    )


def candidate() -> dict:
    return {
        "schema_version": "ai-explanation-output/v1",
        "summary": "本次两个维度可分别观察。",
        "integrated_insights": [
            {
                "kind": "context_dependent_pattern",
                "title": "联合观察",
                "content": "两个维度均有标准描述。",
                "why_it_matters": "分别记录两个维度的表现。",
                "evidence_refs": [{"kind": "dimension", "ref": A}, {"kind": "dimension", "ref": B}],
            }
        ],
        "suggestions": [
            {
                "origin": "generated_low_risk",
                "category": "self_observation",
                "title": "简单记录",
                "goal": "记录观察",
                "actions": ["自愿记录一次观察。"],
                "rationale": "对应本次标准结果。",
                "evidence_refs": [{"kind": "dimension", "ref": A}],
                "source_suggestion_refs": [],
                "caution": "",
            }
        ],
        "limitations": ["仅基于本次测评，不构成诊断或确定性判断。"],
    }


def test_valid_candidate_preserves_empty_array(prepared: PreparedExplanation) -> None:
    result = validate_output(
        json.dumps(candidate(), ensure_ascii=False), prepared, QSOutputParser()
    )
    assert json.loads(result.content_json)["suggestions"][0]["source_suggestion_refs"] == []


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown_field",
        "missing_field",
        "null_array",
        "duplicate_ref",
        "html",
        "unknown_ref",
        "model_result",
        "only_one_dimension",
        "parent_child",
        "second_insight",
        "source_ref",
        "actions",
        "count",
    ],
)
def test_invalid_candidate_rejected(prepared: PreparedExplanation, mutation: str) -> None:
    value = candidate()
    insight = value["integrated_insights"][0]
    suggestion = value["suggestions"][0]
    if mutation == "unknown_field":
        value["unknown"] = "extra"
    elif mutation == "missing_field":
        del suggestion["source_suggestion_refs"]
    elif mutation == "null_array":
        suggestion["source_suggestion_refs"] = None
    elif mutation == "duplicate_ref":
        insight["evidence_refs"][1] = insight["evidence_refs"][0]
    elif mutation == "html":
        value["summary"] = "<b>结论</b>"
    elif mutation == "unknown_ref":
        insight["evidence_refs"][1]["ref"] = "dimension:unknown"
    elif mutation == "model_result":
        suggestion["evidence_refs"] = [{"kind": "model_result", "ref": "model_result"}]
    elif mutation == "only_one_dimension":
        insight["evidence_refs"][1] = {"kind": "overall_result", "ref": "overall_result"}
    elif mutation == "parent_child":
        insight["evidence_refs"][1]["ref"] = "dimension:P"
    elif mutation == "second_insight":
        second = json.loads(json.dumps(insight))
        second["evidence_refs"][1]["ref"] = "dimension:P"
        value["integrated_insights"].append(second)
    elif mutation == "source_ref":
        suggestion["origin"] = "standard_derived"
        suggestion["source_suggestion_refs"] = ["suggestion:unknown"]
    elif mutation == "actions":
        suggestion["actions"] *= 4
    else:
        value["integrated_insights"] *= 4
    with pytest.raises(InvalidOutput):
        validate_output(json.dumps(value, ensure_ascii=False), prepared, QSOutputParser())


@pytest.mark.parametrize(
    "raw", ["[]", "{} {}", '{"summary":"a","summary":"b"}', '{"summary":NaN}', "\ud800"]
)
def test_non_json_or_ambiguous_json_rejected(raw: str) -> None:
    with pytest.raises(InvalidOutput):
        QSOutputParser().parse(raw)


def test_ancestor_and_descendant_rejected(prepared: PreparedExplanation) -> None:
    data = json.loads(prepared.assembled_input.provider_payload)
    next(d for d in data["facts"]["dimensions"] if d["ref"] == B)["parent_ref"] = A
    prepared = replace(
        prepared,
        assembled_input=replace(prepared.assembled_input, provider_payload=json.dumps(data)),
    )
    value = candidate()
    value["integrated_insights"][0]["evidence_refs"] = [
        {"kind": "dimension", "ref": "dimension:P"},
        {"kind": "dimension", "ref": B},
    ]
    with pytest.raises(InvalidOutput, match="ancestor_descendant_combination"):
        validate_output(json.dumps(value), prepared, QSOutputParser())


def test_raw_character_limit(prepared: PreparedExplanation) -> None:
    with pytest.raises(InvalidOutput, match="output_too_long"):
        validate_output(" " * 8001, prepared, QSOutputParser())


@pytest.mark.parametrize("safety", [False, True])
def test_retained_go_deterministic_validation_parity(
    prepared: PreparedExplanation, safety: bool
) -> None:
    values = [candidate() for _ in range(3)]
    values[1]["integrated_insights"][0]["evidence_refs"][1]["ref"] = "dimension:unknown"
    values[2]["integrated_insights"][0]["evidence_refs"][1]["ref"] = "dimension:P"
    if safety:
        values = [candidate() for _ in range(4)]
        values[1]["summary"] = "这导致了另一种结果。"
        values[2]["limitations"] = ["仅基于本次测评。"]
        values[3]["limitations"] = ["本次测评不构成诊断，但属于确定性判断。"]
    request = {
        "Input": json.loads(prepared.assembled_input.canonical_json),
        "Definition": json.loads(prepared.release.definition_json),
        "Candidates": values,
        "Safety": safety,
    }
    # The old Go input encoded absent references as null; normalize only this approved delta.
    for dimension in request["Input"]["facts"]["dimensions"]:
        if dimension["standard_suggestion_refs"] == []:
            dimension["standard_suggestion_refs"] = None
    result = legacy_go_result(request)
    actual = []
    for value in values:
        try:
            checked = validate_output(json.dumps(value), prepared, QSOutputParser())
            if safety:
                check_safety(checked)
            actual.append(True)
        except InvalidOutput:
            actual.append(False)
    assert (
        actual
        == json.loads(result.stdout)
        == ([True, False, False, False] if safety else [True, False, False])
    )
