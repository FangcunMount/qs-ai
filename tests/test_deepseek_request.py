import json
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from qs_ai.application.interpretation.input import assemble_input
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.application.interpretation.provider import ModelRoute, ProviderFailure
from qs_ai.infrastructure.qs_server.deepseek_request import build_request, compatible_schema
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses, parse_response


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


def response_body() -> dict:
    return {
        "id": "response-1",
        "status": "completed",
        "model": "deepseek-v4-pro",
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": '```json\n{"summary":"原文"}\n```'}],
            }
        ],
        "usage": {"input_tokens": 10, "output_tokens": 20},
    }


async def test_single_http_call_and_response_receipt() -> None:
    calls = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer fake-test-key"
        return httpx.Response(200, json=response_body())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = DeepSeekResponses(client, "https://provider.invalid/responses", "fake-test-key")
        result = await adapter.generate(prepared(), route(), schema(), "invocation-1")
    assert len(calls) == 1
    assert result.request_id == "response-1" and result.input_tokens == 10
    assert result.normalization == "markdown_unwrapped"
    assert result.raw_output.startswith("```json")
    assert json.loads(result.validation_output) == {"summary": "原文"}


@pytest.mark.parametrize(
    "status,code,unknown",
    [
        (429, "provider_rate_limited", False),
        (401, "provider_authentication_failed", False),
        (403, "provider_authentication_failed", False),
        (408, "provider_server_error", True),
        (500, "provider_server_error", True),
        (503, "provider_server_error", True),
        (302, "provider_request_rejected", False),
    ],
)
async def test_http_failure_is_sanitized_and_not_replayed(
    status: int, code: str, unknown: bool
) -> None:
    calls = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            status, text="private provider detail", headers={"Location": "https://other.invalid"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = DeepSeekResponses(client, "https://provider.invalid/responses", "fake-test-key")
        with pytest.raises(ProviderFailure) as error:
            await adapter.generate(prepared(), route(), schema(), "invocation-1")
    assert error.value.code == code and error.value.result_unknown == unknown
    assert str(error.value) == code and len(calls) == 1


async def test_read_timeout_is_unknown_without_retry() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("sensitive request information")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = DeepSeekResponses(client, "https://provider.invalid/responses", "fake-test-key")
        with pytest.raises(ProviderFailure, match="provider_timeout") as error:
            await adapter.generate(prepared(), route(), schema(), "invocation-1")
    assert error.value.result_unknown


async def test_response_limit_is_enforced() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"x" * 101))
    ) as client:
        adapter = DeepSeekResponses(
            client, "https://provider.invalid/responses", "fake-test-key", max_response_bytes=100
        )
        with pytest.raises(ProviderFailure, match="provider_response_too_large"):
            await adapter.generate(prepared(), route(), schema(), "invocation-1")


@pytest.mark.parametrize(
    "change,code",
    [
        ({"model": "other"}, "provider_model_mismatch"),
        ({"id": ""}, "provider_response_id_missing"),
        ({"status": "in_progress"}, "provider_response_not_terminal"),
        (
            {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}},
            "provider_output_token_limit",
        ),
        ({"output": []}, "provider_output_cardinality_invalid"),
        ({"usage": {"input_tokens": -1, "output_tokens": 1}}, "provider_usage_invalid"),
    ],
)
def test_response_contract_failure(change: dict, code: str) -> None:
    value = response_body() | change
    with pytest.raises(ProviderFailure, match=code):
        parse_response(json.dumps(value).encode(), route(), "invocation", 1)


def test_missing_usage_is_unknown_not_zero() -> None:
    value = response_body()
    del value["usage"]
    result = parse_response(json.dumps(value).encode(), route(), "invocation", 1)
    assert result.input_tokens is None and result.output_tokens is None


@pytest.mark.parametrize(
    "payload",
    [b'{"status":"completed","status":"failed"}', b'{"usage":NaN}', b"not-json", b"\xff"],
)
def test_raw_response_must_not_be_normalized_before_strict_parsing(payload: bytes) -> None:
    with pytest.raises(ProviderFailure, match="provider_response_invalid"):
        parse_response(payload, route(), "invocation", 1)


@pytest.mark.parametrize(
    "error_type,code,unknown",
    [
        (httpx.ConnectError, "provider_connect_failed", False),
        (httpx.ConnectTimeout, "provider_connect_failed", False),
        (httpx.ReadTimeout, "provider_timeout", True),
        (httpx.WriteTimeout, "provider_timeout", True),
        (httpx.ReadError, "provider_transport_error", True),
    ],
)
async def test_transport_errors_preserve_dispatch_uncertainty(error_type, code, unknown):
    calls = 0

    def handle(request):
        nonlocal calls
        calls += 1
        raise error_type("private provider detail", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        gateway = DeepSeekResponses(client, "https://provider.invalid/responses", "test-key")
        with pytest.raises(ProviderFailure) as caught:
            await gateway.generate(prepared(), route(), schema(), "invocation")
    assert calls == 1
    assert caught.value.code == code
    assert caught.value.result_unknown is unknown
    assert str(caught.value) == code


async def test_cancelled_dispatch_is_propagated_without_retry():
    import asyncio

    started = asyncio.Event()
    calls = 0

    async def handle(request):
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        gateway = DeepSeekResponses(client, "https://provider.invalid/responses", "test-key")
        task = asyncio.create_task(gateway.generate(prepared(), route(), schema(), "invocation"))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert calls == 1
