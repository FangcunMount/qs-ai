import json
from dataclasses import replace

import httpx
import pytest
from langsmith import get_tracing_context

from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.config import Settings
from qs_ai.infrastructure.models.router import ModelGatewayRouter
from qs_ai.infrastructure.models.zhipu import parse_chat
from tests.test_deepseek_request import prepared, schema
from tests.test_model_route_v2 import route


def settings():
    return Settings(
        zhipu_api_key="synthetic-zhipu",
        models={
            "bindings": [
                {
                    "binding_id": "zhipu-official",
                    "revision": "v1",
                    "provider": "zhipu",
                    "protocol": "chat_completions",
                    "adapter_contract": "zhipu-chat/v1",
                    "endpoint": "https://open.bigmodel.cn/api/paas/v4/chat/completions",
                    "credential_slot": "zhipu",
                }
            ]
        },
    )


def response():
    return {
        "id": "receipt-1",
        "model": "glm-5.3",
        "choices": [{"finish_reason": "stop", "message": {"content": '{"ok":true}'}}],
    }


async def test_frozen_binding_runs_without_current_catalog_and_never_traces(monkeypatch):
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    calls = []

    def handle(request):
        assert get_tracing_context()["enabled"] is False
        assert request.headers["authorization"] == "Bearer synthetic-zhipu"
        calls.append(json.loads(request.content))
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        result = await ModelGatewayRouter(client, settings()).generate_messages(
            prepared().messages, route(), schema(), "invocation-1"
        )
    assert len(calls) == 1
    assert [m["role"] for m in calls[0]["messages"]] == ["system", "user"]
    assert prepared().messages.data_json in calls[0]["messages"][1]["content"]
    assert result.input_tokens is None and result.output_tokens is None
    assert result.request_id == "receipt-1"


@pytest.mark.parametrize("status", [401, 403, 429, 500])
async def test_error_is_not_retried_or_switched(status):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(status, text="sensitive upstream body")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ProviderFailure) as error:
            await ModelGatewayRouter(client, settings()).generate_messages(
                prepared().messages, route(), schema(), "invocation-1"
            )
    assert len(calls) == 1
    assert "sensitive" not in str(error.value)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(model="other"),
        lambda v: v["choices"][0].update(finish_reason="length"),
        lambda v: v["choices"][0]["message"].update(content='{"a":1,"a":2}'),
        lambda v: v.update(usage={"prompt_tokens": True}),
    ],
)
def test_invalid_receipt_is_rejected(mutation):
    value = response()
    mutation(value)
    with pytest.raises(ProviderFailure):
        parse_chat(json.dumps(value).encode(), route(), "invocation-1", 1)


async def test_changed_binding_revision_does_not_use_latest():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("must not dispatch"))
    ) as client:
        with pytest.raises(ProviderFailure):
            await ModelGatewayRouter(client, settings()).generate_messages(
                prepared().messages,
                replace(route(), binding_revision="missing"),
                schema(),
                "invocation-1",
            )


async def test_deepseek_legacy_wire_and_credentials_remain_separate():
    import asyncio

    from qs_ai.infrastructure.qs_server.deepseek_request import build_messages_request
    from tests.test_deepseek_request import response_body
    from tests.test_deepseek_request import route as deepseek_route

    values = settings().model_copy(
        update={
            "generation": settings().generation.model_copy(
                update={"endpoint": "https://api.deepseek.com/responses"}
            ),
        }
    )
    from pydantic import SecretStr

    values = values.model_copy(update={"deepseek_api_key": SecretStr("synthetic-deepseek")})
    calls = {}

    def handle(request):
        calls[request.url.host] = json.loads(request.content)
        if request.url.host == "api.deepseek.com":
            assert request.headers["authorization"] == "Bearer synthetic-deepseek"
            return httpx.Response(200, json=response_body())
        assert request.headers["authorization"] == "Bearer synthetic-zhipu"
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        gateway = ModelGatewayRouter(client, values)
        await asyncio.gather(
            *(
                gateway.generate_messages(prepared().messages, r, schema(), "invocation-1")
                for r in (deepseek_route(), route())
            )
        )
    assert calls["api.deepseek.com"] == build_messages_request(
        prepared().messages, deepseek_route(), schema()
    )
    assert calls["open.bigmodel.cn"]["model"] == "glm-5.3"
