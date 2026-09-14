import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from qs_ai.application.interpretation.input import (
    InputPolicy,
    InvalidInput,
    NotApplicable,
    assemble_input,
)
from qs_ai.application.interpretation.prompts import RenderPolicy, render_prompt
from qs_ai.infrastructure.qs_server.prompts import load_prompt, prompt_directory
from tests.legacy_go_baseline import legacy_go_result

FIXTURE = Path(__file__).parent / "fixtures" / "report_snapshot.json"


@pytest.fixture
def policy() -> InputPolicy:
    return InputPolicy(
        "participant-scale-score-range-default",
        "v6",
        "sha256:cc747df0a6ae4b02b65b7447ce08845fa8c4e91b9d674f51ac25fbcbffd2a2a2",
        None,
        None,
        2,
        12,
        (),
        (),
        ("sleep_routine",),
        True,
        False,
    )


def test_assembles_refs_nulls_order_and_deduplicated_suggestions(policy: InputPolicy) -> None:
    result = assemble_input(FIXTURE.read_text(), policy, focus_areas=("sleep_routine",))
    document = json.loads(result.canonical_json)
    provider = json.loads(result.provider_payload)
    assert set(provider) == {"context", "facts"}
    assert provider["facts"] == document["facts"]
    assert document["source"]["report_id"] == "99"
    assert (
        result.fingerprint == "sha256:" + hashlib.sha256(result.canonical_json.encode()).hexdigest()
    )
    dimensions = provider["facts"]["dimensions"]
    assert [d["code"] for d in dimensions] == ["P", "B", "A/观察"]
    a = dimensions[2]
    assert a["ref"] == "dimension:A%2F%E8%A7%82%E5%AF%9F"
    assert a["parent_ref"] == "dimension:P"
    assert a["standard_suggestion_refs"] == ["suggestion:report:2"]
    assert dimensions[1]["standard_suggestion_refs"] == []
    assert dimensions[1]["level"] is None
    assert dimensions[1]["raw_score"]["max"] is None
    assert len(provider["facts"]["standard_suggestions"]) == 2
    assert "{{locale}}" in a["standard_description"]


def test_eligibility_excludes_parent_and_norm_without_fabrication(policy: InputPolicy) -> None:
    result = assemble_input(
        FIXTURE.read_text(), replace(policy, excluded_codes=("P",), include_norm_context=False)
    )
    dimensions = json.loads(result.provider_payload)["facts"]["dimensions"]
    assert len(dimensions) == 2
    assert dimensions[1]["parent_ref"] is None
    assert dimensions[1]["norm_context"] is None
    assert dimensions[1]["derived_scores"][0]["value"] == 50


def test_assembled_input_renders_with_existing_v6(policy: InputPolicy) -> None:
    baseline = json.loads((prompt_directory() / "published-profile-baseline.json").read_text())
    definition = baseline["profiles"][0]["definition"]
    generation, insight, suggestion = (
        definition[k] for k in ("generation_policy", "insight_policy", "suggestion_policy")
    )
    render_policy = RenderPolicy(
        generation["prompt_template_id"],
        generation["prompt_version"],
        policy.allowed_focus_areas,
        tuple(insight["allowed_kinds"]),
        insight["min_items"],
        insight["max_items"],
        insight["min_dimension_refs_per_item"],
        insight["max_dimension_refs_per_item"],
        False,
        tuple(suggestion["allowed_origins"]),
        tuple(suggestion["allowed_categories"]),
        suggestion["min_items"],
        suggestion["max_items"],
        suggestion["max_actions_per_item"],
        generation["max_output_characters"],
    )
    assembled = assemble_input(FIXTURE.read_text(), policy)
    messages = render_prompt(
        load_prompt(render_policy.template_id, "v6"), render_policy, assembled.provider_payload
    )
    assert "{{" not in messages.task_message
    assert "标准描述 {{locale}}" not in messages.system_message + messages.task_message
    assert "标准描述 {{locale}}" in messages.data_json
    assert "source" not in json.loads(messages.data_json)
    assert "profile" not in json.loads(messages.data_json)
    assert json.loads(messages.data_json) == json.loads(assembled.provider_payload)


@pytest.mark.parametrize(
    "change",
    [
        {"min_dimensions": 4},
        {"max_dimensions": 2},
        {"eligible_codes": ("B",)},
        {"model_code": "wrong"},
        {"model_version": "v2"},
    ],
)
def test_not_applicable(policy: InputPolicy, change: dict) -> None:
    with pytest.raises(NotApplicable):
        assemble_input(FIXTURE.read_text(), replace(policy, **change))


@pytest.mark.parametrize(
    "mutation",
    ["old_schema", "duplicate", "negative_order", "html", "bool_score", "nan", "unknown_source"],
)
def test_invalid_snapshot_rejected(policy: InputPolicy, mutation: str) -> None:
    data = json.loads(FIXTURE.read_text())
    if mutation == "old_schema":
        data["schema_version"] = "legacy"
    elif mutation == "duplicate":
        data["dimensions"].append(data["dimensions"][0])
    elif mutation == "negative_order":
        data["dimensions"][0]["sort_order"] = -1
    elif mutation == "html":
        data["dimensions"][0]["description"] = "<script>"
    elif mutation == "bool_score":
        data["dimensions"][0]["raw_score"] = True
    elif mutation == "nan":
        data["dimensions"][0]["raw_score"] = float("nan")
    else:
        data["source"]["report_id"] = ""
    with pytest.raises(InvalidInput):
        assemble_input(json.dumps(data), policy)


def test_context_and_duplicate_json_rejected(policy: InputPolicy) -> None:
    for focus in (("unknown",), ("sleep_routine", "sleep_routine")):
        with pytest.raises(InvalidInput):
            assemble_input(FIXTURE.read_text(), policy, focus_areas=focus)
    with pytest.raises(InvalidInput):
        assemble_input('{"schema_version":"a","schema_version":"b"}', policy)


@pytest.mark.parametrize("filter_parent", [False, True])
def test_retained_go_input_assembly_parity(policy: InputPolicy, filter_parent: bool) -> None:
    baseline = json.loads((prompt_directory() / "published-profile-baseline.json").read_text())
    definition = baseline["profiles"][0]["definition"]
    if filter_parent:
        definition["eligibility"]["excluded_dimension_codes"] = ["P"]
        definition["input_policy"]["include_norm_context"] = False
        policy = replace(policy, excluded_codes=("P",), include_norm_context=False)
    fingerprint = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                definition, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode()
        ).hexdigest()
    )
    policy = replace(policy, profile_fingerprint=fingerprint)
    request = {
        "Definition": definition,
        "Snapshot": json.loads(FIXTURE.read_text()),
        "Focus": ["sleep_routine"],
    }
    original = legacy_go_result(request)
    actual = assemble_input(FIXTURE.read_text(), policy, focus_areas=("sleep_routine",))
    legacy = json.loads(original.stdout)
    # Normalize the approved Input v1 correction; keep the original Go bytes in the fixture.
    empty_refs = 0
    for dimension in legacy["facts"]["dimensions"]:
        if dimension["standard_suggestion_refs"] is None:
            empty_refs += 1
            dimension["standard_suggestion_refs"] = []
    assert empty_refs > 0
    assert json.loads(actual.canonical_json) == legacy
