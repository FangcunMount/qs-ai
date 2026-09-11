import asyncio
import json
import time
from typing import Any
from urllib.parse import urlsplit

import httpx

from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute, ProviderFailure
from qs_ai.infrastructure.qs_server.deepseek_request import build_messages_request, build_request
from qs_ai.infrastructure.qs_server.normalization import normalize_output


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate field")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise ValueError("Invalid JSON constant")


def parse_response(
    raw: bytes, route: ModelRoute, invocation_id: str, latency: int
) -> ModelResponse:
    try:
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
        status = data["status"]
        if status != "completed":
            detail = data.get("incomplete_details") or {}
            error = data.get("error") or {}
            reason = detail.get("reason", "").strip().lower()
            code = error.get("code", "").strip().lower()
            if reason == "content_filter" or code in {
                "content_filter",
                "safety",
                "policy_violation",
            }:
                raise ProviderFailure("provider_refusal")
            if status == "incomplete" and reason == "max_output_tokens":
                raise ProviderFailure("provider_output_token_limit")
            if status in {"queued", "in_progress"}:
                raise ProviderFailure("provider_response_not_terminal", result_unknown=True)
            if status == "failed":
                raise ProviderFailure(
                    "provider_response_failed",
                    retryable=code
                    in {"server_error", "rate_limit_exceeded", "temporarily_unavailable"},
                )
            if status in {"incomplete", "cancelled"}:
                raise ProviderFailure(
                    "provider_response_" + status, retryable=status == "cancelled"
                )
            raise ProviderFailure("provider_response_status_invalid")
        if not isinstance(data["id"], str) or not data["id"].strip():
            raise ProviderFailure("provider_response_id_missing")
        if data["model"] != route.model:
            raise ProviderFailure("provider_model_mismatch")
        messages = [item for item in data["output"] if item["type"] == "message"]
        if len(messages) != 1:
            raise ProviderFailure("provider_output_cardinality_invalid")
        parts = messages[0]["content"]
        if any(part["type"] == "refusal" for part in parts):
            raise ProviderFailure("provider_refusal")
        texts = [part["text"] for part in parts if part["type"] == "output_text"]
        if (
            not texts
            or any(not isinstance(text, str) for text in texts)
            or not "".join(texts).strip()
        ):
            raise ProviderFailure("provider_output_cardinality_invalid")
        normalized = normalize_output(route.provider, "".join(texts))
        usage = data.get("usage")
        counts: list[int | None] = [None, None]
        if usage is not None:
            counts = [usage["input_tokens"], usage["output_tokens"]]
            reasoning = (usage.get("output_tokens_details") or {}).get("reasoning_tokens", 0)
            if (
                any(type(value) is not int or value < 0 for value in (*counts, reasoning))
                or reasoning > counts[1]
            ):
                raise ProviderFailure("provider_usage_invalid")
        return ModelResponse(
            invocation_id,
            data["id"],
            data["model"],
            normalized.raw_output,
            normalized.validation_output,
            normalized.normalization,
            counts[0],
            counts[1],
            latency,
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ProviderFailure("provider_response_invalid") from None


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
        self._client, self._endpoint, self._api_key = client, endpoint, api_key
        self._limit = max_response_bytes

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
