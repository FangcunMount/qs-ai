from typing import Any
from urllib.parse import urlsplit

import httpx
from langchain_core.messages import ChatMessage, HumanMessage, SystemMessage
from langsmith import tracing_context

from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute
from qs_ai.infrastructure.models.response_contract import parse_response as parse_response
from qs_ai.infrastructure.models.responses import QSResponsesModel
from qs_ai.infrastructure.qs_server.deepseek_request import build_messages_request, build_request


class DeepSeekResponses:
    def __init__(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        api_key: str,
        *,
        max_response_bytes: int = 2_097_152,
    ) -> None:
        url = urlsplit(endpoint)
        if (
            url.scheme not in {"https", "http"}
            or not url.hostname
            or url.username
            or url.password
            or url.fragment
        ):
            raise ValueError("Invalid provider endpoint")
        if not api_key.strip() or max_response_bytes < 1:
            raise ValueError("Provider credential and response limit are required")
        self._model = QSResponsesModel(client, endpoint, api_key, max_response_bytes)

    async def generate(
        self,
        prepared: PreparedExplanation,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse:
        if not invocation_id:
            raise ValueError("Invocation identity is required")
        body = build_request(prepared, route, schema)
        return await self._send(body, route, invocation_id)

    async def generate_messages(
        self,
        messages: PromptMessages,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse:
        if not invocation_id:
            raise ValueError("Invocation identity is required")
        return await self._send(
            build_messages_request(messages, route, schema), route, invocation_id
        )

    async def _send(
        self, body: dict[str, Any], route: ModelRoute, invocation_id: str
    ) -> ModelResponse:
        messages = [
            SystemMessage(content=body["instructions"]),
            ChatMessage(role="developer", content=body["input"][0]["content"][0]["text"]),
            HumanMessage(content=body["input"][1]["content"][0]["text"]),
        ]
        # Assessment content must never activate ambient cloud tracing.
        with tracing_context(enabled=False):
            response = await self._model.ainvoke(
                messages,
                config={"callbacks": []},
                body=body,
                route=route,
                invocation_id=invocation_id,
            )
        return ModelResponse(**response.response_metadata["qs_receipt"])
