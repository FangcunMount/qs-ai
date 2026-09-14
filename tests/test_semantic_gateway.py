import json
from dataclasses import replace

import httpx
import pytest

from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from tests.evaluation_helpers import prepare_semantic_messages
from tests.test_deepseek_request import prepared, response_body, route, schema
from tests.test_semantic_input import inputs


async def test_semantic_messages_use_same_transport_with_independent_route():
    bound, generation, assertions = inputs()
    messages = prepare_semantic_messages(bound, generation, assertions)
    judge_route = replace(route(), route="semantic_judge", revision="v1")
    calls = []

    def handle(request):
        body = json.loads(request.content)
        calls.append(body)
        assert body["instructions"] == messages.system_message
        assert body["input"][0]["content"][0]["text"] == messages.task_message
        assert (
            body["input"][1]["content"][0]["text"]
            == messages.data_preamble + "\n\n" + messages.data_json
        )
        assert "scores" in body["text"]["format"]["schema"]["properties"]
        return httpx.Response(200, json=response_body())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        gateway = DeepSeekResponses(client, "https://provider.invalid/responses", "test-key")
        result = await gateway.generate_messages(
            messages,
            judge_route,
            json.loads(load_semantic_assets().output_schema_json),
            "judge-call:1",
        )
        assert result.invocation_id == "judge-call:1"
        with pytest.raises(ValueError, match="published Profile"):
            await gateway.generate(prepared(), judge_route, schema(), "gen-call:1")
    assert len(calls) == 1


async def test_semantic_timeout_remains_unknown_and_is_never_retried_by_transport():
    bound, generation, assertions = inputs()
    calls = []

    def handle(request):
        calls.append(request)
        raise httpx.ReadTimeout("simulated", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        gateway = DeepSeekResponses(client, "https://provider.invalid/responses", "test-key")
        with pytest.raises(ProviderFailure) as exc:
            await gateway.generate_messages(
                prepare_semantic_messages(bound, generation, assertions),
                route(),
                json.loads(load_semantic_assets().output_schema_json),
                "judge-call:1",
            )
    assert exc.value.result_unknown is True
    assert len(calls) == 1
