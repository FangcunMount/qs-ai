import json

import pytest

from qs_ai.infrastructure.qs_server.normalization import normalize_output


@pytest.mark.parametrize(
    "raw",
    [
        '```json\n{"summary":"原文"}\n```',
        '{"parameters":{"summary":"原文"}}',
        '{"json":{"summary":"原文"}}',
        json.dumps({"json_string": '{"summary":"原文"}'}),
        '```JSON\n{"parameters":{"summary":"原文"}}\n```',
    ],
)
def test_reviewed_wrappers_preserve_raw_and_do_not_add_fields(raw: str) -> None:
    result = normalize_output("deepseek", raw)
    assert result.raw_output == raw
    assert json.loads(result.validation_output) == {"summary": "原文"}
    assert result.normalization != "unchanged"


@pytest.mark.parametrize(
    "raw",
    [
        "prefix ```json\n{}\n```",
        "```json\n{broken}\n```",
        "```json\n{}\n``` suffix",
        "```\n{}\n```",
        '{"json":{},"extra":1}',
        '{"json":[]}',
        '{"json_string":"broken"}',
        '{"json":{},"json":{}}',
        '{"json":{"value":NaN}}',
        "[]",
        '{"summary":"原文"}',
    ],
)
def test_ambiguous_and_malformed_output_unchanged(raw: str) -> None:
    result = normalize_output("deepseek", raw)
    assert result.validation_output == raw
    assert result.normalization == "unchanged"


def test_not_applied_to_other_providers() -> None:
    raw = "```json\n{}\n```"
    assert normalize_output("openai", raw).validation_output == raw


def test_only_one_wrapper_layer_removed() -> None:
    result = normalize_output("deepseek", '{"json":{"parameters":{}}}')
    assert json.loads(result.validation_output) == {"parameters": {}}


def test_enclosed_formatting_preserved_for_character_budget() -> None:
    inner = '{  "summary": "原文"  }'
    result = normalize_output("deepseek", '{ "json" : ' + inner + " }")
    assert result.validation_output == inner
