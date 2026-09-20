import json

import httpx
from langchain_core.language_models import BaseChatModel
from langsmith import get_tracing_context

from qs_ai.infrastructure.models.responses import QSResponsesModel
from qs_ai.infrastructure.qs_server.deepseek_request import build_request
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses
from tests.test_deepseek_request import prepared, response_body, route, schema


async def test_langchain_model_preserves_wire_contract_and_disables_ambient_tracing(monkeypatch):
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    monkeypatch.setenv("LANGSMITH_API_KEY", "test-must-not-be-used")
    calls = []

    def handle(request):
        assert get_tracing_context()["enabled"] is False
        calls.append(json.loads(request.content))
        return httpx.Response(200, json=response_body())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        gateway = DeepSeekResponses(client, "https://provider.invalid/responses", "private-key")
        assert isinstance(gateway._model, BaseChatModel)
        assert isinstance(gateway._model, QSResponsesModel)
        assert "private-key" not in repr(gateway._model)
        result = await gateway.generate(prepared(), route(), schema(), "invocation")
    assert calls == [build_request(prepared(), route(), schema())]
    assert result.request_id == "response-1"
    assert result.invocation_id == "invocation"
    assert result.input_tokens == 10
    assert result.output_tokens == 20
    assert result.raw_output == response_body()["output"][0]["content"][0]["text"]
