from dataclasses import replace

import pytest

from qs_ai.application.evaluation.model_response import response_evidence
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy
from tests.test_deepseek_request import route, schema


@pytest.mark.parametrize("stage", ["generation", "semantic"])
@pytest.mark.parametrize("raw", ["not-json", "{}", "NaN", '{"x":1,"x":2}'])
def test_invalid_output_becomes_terminal_contract_failure(stage, raw):
    response = ModelResponse("call:1", "request:1", route().model, raw, raw, "none", 1, 2, 10)
    result = response_evidence(
        stage, "execution:1", "call:1", route(), schema(), response, max_output_characters=8000
    )
    assert result.failure is not None
    assert result.raw == raw.encode()
    policy = load_execution_policy()
    if stage == "generation":
        assert result.failure.disposition == "replace_generation"
        assert policy.allows_automatic_generation_recovery(result.failure)
    else:
        assert result.failure.code == "semantic_output_schema_invalid"
        assert policy.allows_automatic_semantic_recovery(result.failure)


def test_invalid_receipt_does_not_get_contract_replacement_permission():
    response = ModelResponse("other:1", "request:1", route().model, "{}", "{}", "none", 1, 2, 10)
    result = response_evidence(
        "generation",
        "execution:1",
        "call:1",
        route(),
        schema(),
        response,
        max_output_characters=8000,
    )
    assert result.receipt is None
    assert result.failure.code == "provider_receipt_invalid"
    assert not load_execution_policy().allows_automatic_generation_recovery(result.failure)
    response = replace(response, invocation_id="call:1", input_tokens=True)
    assert (
        response_evidence(
            "generation",
            "execution:1",
            "call:1",
            route(),
            schema(),
            response,
            max_output_characters=8000,
        ).receipt
        is None
    )


def test_oversized_response_is_classified_without_storing_unbounded_bytes():
    response = ModelResponse(
        "call:1", "request:1", route().model, "x" * (256 * 1024 + 1), "{}", "none", 1, 2, 10
    )
    result = response_evidence(
        "semantic", "execution:1", "call:1", route(), schema(), response, max_output_characters=8000
    )
    assert result.raw == result.normalized == b""
    assert result.failure.code == "semantic_output_missing_or_too_large"


def test_unknown_usage_and_execution_identity_survive_evaluation_receipt():
    from qs_ai.infrastructure.models.router import ModelGatewayRouter
    from qs_ai.infrastructure.persistence.mysql.evaluation_projection import decode_receipt
    from tests.test_model_route_v2 import route as v2_route

    frozen = v2_route()
    response = ModelResponse(
        "call:1", "request:1", frozen.model, "{}", "{}", "none", None, None, 10
    )
    response = ModelGatewayRouter._with_identity(response, frozen)
    result = response_evidence(
        "generation", "execution:1", "call:1", frozen, {}, response, max_output_characters=8000
    )
    assert result.failure is None
    receipt = result.receipt
    assert receipt.input_tokens is None and receipt.output_tokens is None
    assert receipt.execution_identity.route_fingerprint == frozen.fingerprint()
    assert decode_receipt(receipt.definition()) == receipt


@pytest.mark.parametrize("limit", [8000, 3000])
@pytest.mark.parametrize("extra", [0, 1])
def test_generation_checks_frozen_character_limit_before_compaction(limit, extra):
    from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy

    # Valid JSON with formatting whitespace; UTF-8 bytes exceed character count.
    compact = '{"text":"中文"}'
    raw = compact + "\n" * (limit + extra - len(compact))
    response = ModelResponse("call:1", "request:1", route().model, raw, raw, "none", 1, 2, 10)
    result = response_evidence(
        "generation",
        "execution:1",
        "call:1",
        route(),
        {},
        response,
        max_output_characters=limit,
    )
    assert result.raw == result.normalized == raw.encode()
    assert result.receipt is not None
    if extra:
        assert result.failure is not None
        assert result.failure.code == "output_too_long"
        assert load_execution_policy().allows_automatic_generation_recovery(result.failure)
    else:
        assert result.failure is None


def test_generation_character_limit_does_not_apply_to_semantic_output():
    raw = '{"text":"中文"}' + "\n" * 8000
    response = ModelResponse("call:1", "request:1", route().model, raw, raw, "none", 1, 2, 10)
    result = response_evidence(
        "semantic",
        "execution:1",
        "call:1",
        route(),
        {},
        response,
        max_output_characters=8000,
    )
    assert result.failure is None
