"""Async LangChain model for the frozen QS Responses wire contract."""

import asyncio
import time
from dataclasses import asdict
from typing import Any

import httpx
from langchain_core.callbacks import AsyncCallbackManagerForLLMRun, CallbackManagerForLLMRun
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ChatMessage, HumanMessage, SystemMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from pydantic import PrivateAttr

from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute, ProviderFailure
from qs_ai.infrastructure.models.response_contract import parse_response


class QSResponsesModel(BaseChatModel):
    _client: httpx.AsyncClient = PrivateAttr()
    _endpoint: str = PrivateAttr()
    _api_key: str = PrivateAttr()
    _limit: int = PrivateAttr()

    def __init__(self, client: httpx.AsyncClient, endpoint: str, api_key: str, limit: int) -> None:
        super().__init__(cache=False, disable_streaming=True)
        self._client, self._endpoint, self._api_key, self._limit = client, endpoint, api_key, limit

    @property
    def _llm_type(self) -> str:
        return "qs-responses"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        raise NotImplementedError("QS model requires asynchronous invocation")

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        if stop is not None or len(messages) != 3:
            raise ValueError("Unsupported QS message contract")
        system, developer, user = messages
        if not (
            isinstance(system, SystemMessage)
            and isinstance(developer, ChatMessage)
            and developer.role == "developer"
            and isinstance(user, HumanMessage)
        ):
            raise ValueError("Unsupported QS message roles")
        if any(not isinstance(message.content, str) for message in messages):
            raise ValueError("QS messages must contain text")
        body = dict(kwargs["body"])
        body["instructions"] = system.content
        body["input"] = [
            {"role": "developer", "content": [{"type": "input_text", "text": developer.content}]},
            {"role": "user", "content": [{"type": "input_text", "text": user.content}]},
        ]
        receipt = await self._send(body, kwargs["route"], kwargs["invocation_id"])
        message = AIMessage(
            content=receipt.raw_output, response_metadata={"qs_receipt": asdict(receipt)}
        )
        return ChatResult(generations=[ChatGeneration(message=message)])

    async def _send(
        self, body: dict[str, Any], route: ModelRoute, invocation_id: str
    ) -> ModelResponse:
        started = time.monotonic()
        try:
            async with asyncio.timeout(route.timeout_milliseconds / 1000):
                async with self._client.stream(
                    "POST",
                    self._endpoint,
                    json=body,
                    headers={
                        "Authorization": "Bearer " + self._api_key,
                        "Accept": "application/json",
                    },
                    follow_redirects=False,
                    timeout=route.timeout_milliseconds / 1000,
                ) as response:
                    if not 200 <= response.status_code < 300:
                        status = response.status_code
                        if status == 429:
                            raise ProviderFailure("provider_rate_limited", retryable=True)
                        if status in {401, 403}:
                            raise ProviderFailure("provider_authentication_failed")
                        if status >= 500 or status == 408:
                            raise ProviderFailure(
                                "provider_server_error", retryable=True, result_unknown=True
                            )
                        raise ProviderFailure("provider_request_rejected")
                    payload = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        payload.extend(chunk)
                        if len(payload) > self._limit:
                            raise ProviderFailure(
                                "provider_response_too_large", result_unknown=True
                            )
            return parse_response(
                bytes(payload), route, invocation_id, int((time.monotonic() - started) * 1000)
            )
        except (httpx.ConnectError, httpx.ConnectTimeout):
            raise ProviderFailure("provider_connect_failed", retryable=True) from None
        except (TimeoutError, httpx.TimeoutException):
            raise ProviderFailure("provider_timeout", retryable=True, result_unknown=True) from None
        except httpx.HTTPError:
            raise ProviderFailure(
                "provider_transport_error", retryable=True, result_unknown=True
            ) from None
