import json
from dataclasses import replace
from pathlib import Path

import pytest

from qs_ai.application.interpretation.input import assemble_input
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.infrastructure.qs_server.deepseek_request import build_request, compatible_schema
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt


def route() -> ModelRoute:
    return ModelRoute(
        "balanced_text_v1",
        "v8",
        "deepseek",
        "deepseek-v4-pro",
        "responses",
        "json_schema",
        120000,
        12000,
        "none",
    )


def prepared() -> PreparedExplanation:
    release = load_migrated_release("participant-scale-score-range-default", "v6")
    assembled = assemble_input(
        (Path(__file__).parent / "fixtures" / "report_snapshot.json").read_text(),
        release.input_policy,
    )
    package = load_prompt(release.render_policy.template_id, "v6")
    return PreparedExplanation(
        assembled,
        render_prompt(package, release.render_policy, assembled.provider_payload),
        release,
        package.fingerprint,
    )


def schema() -> dict:
    return json.loads((schema_directory() / "ai-explanation-output-v1.schema.json").read_text())


def test_production_route_request_projection() -> None:
    source = prepared()
    result = build_request(source, route(), schema())
    assert result["model"] == "deepseek-v4-pro"
    assert result["max_output_tokens"] == 12000
    assert result["reasoning"] == {"effort": "none"}
    assert result["instructions"] == source.messages.system_message
    assert result["input"][0]["role"] == "developer"
    assert source.messages.data_json in result["input"][1]["content"][0]["text"]
    assert "store" not in result and "strict" not in result["text"]["format"]
    assert result["text"]["format"]["name"] == "AIExplanationOutput_v1"
    assert "$ref" not in json.dumps(result["text"]["format"]["schema"])


def test_schema_projection_keeps_fields_without_mutating_full_schema() -> None:
    original = schema()
    result = compatible_schema(original)
    suggestion = result["properties"]["suggestions"]["items"]
    assert suggestion["required"] == sorted(original["$defs"]["suggestion"]["properties"])
    assert suggestion["additionalProperties"] is False
    assert "maxItems" not in result["properties"]["suggestions"]
    assert original["properties"]["suggestions"]["maxItems"] == 8
    assert result["properties"]["schema_version"]["enum"] == ["ai-explanation-output/v1"]


@pytest.mark.parametrize(
    "change",
    [
        {"provider": "openai"},
        {"protocol": "other"},
        {"route": "other"},
        {"model": ""},
        {"max_output_tokens": 0},
        {"reasoning_effort": "unknown"},
        {"timeout_milliseconds": False},
    ],
)
def test_invalid_or_mismatched_route_rejected(change: dict) -> None:
    with pytest.raises(ValueError):
        build_request(prepared(), replace(route(), **change), schema())


def test_json_object_mode_and_no_reasoning_omission() -> None:
    value = build_request(
        prepared(),
        replace(route(), structured_output_mode="json_object", reasoning_effort=""),
        schema(),
    )
    assert value["text"]["format"] == {"type": "json_object"}
    assert "reasoning" not in value


def test_route_changes_produce_different_execution_fingerprints() -> None:
    baseline = route()
    assert baseline.fingerprint() == route().fingerprint()
    assert (
        len(
            {
                baseline.fingerprint(),
                replace(baseline, revision="v9").fingerprint(),
                replace(baseline, max_output_tokens=8000).fingerprint(),
                replace(baseline, structured_output_mode="json_object").fingerprint(),
            }
        )
        == 4
    )


def test_cyclic_schema_ref_fails_closed() -> None:
    with pytest.raises(ValueError):
        compatible_schema({"$ref": "#/$defs/a", "$defs": {"a": {"$ref": "#/$defs/a"}}})
