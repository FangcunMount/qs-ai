"""GLM chat contract. The durable caller owns retries, never the adapter."""

import json
from dataclasses import asdict
from typing import Any

from langchain_core.callbacks import AsyncCallbackManagerForLLMRun
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute, ProviderFailure
from qs_ai.infrastructure.models.response_contract import _constant, _pairs
from qs_ai.infrastructure.models.responses import QSResponsesModel


def build_chat(
    messages: PromptMessages, route: ModelRouteV2, schema: dict[str, Any], invocation: str
) -> dict[str, Any]:
    if route.provider != "zhipu" or route.adapter_contract != "zhipu-chat/v1":
        raise ValueError("Unsupported Zhipu route")
    if not 6 <= len(invocation) <= 64:
        raise ValueError("Invalid invocation identity")
    # Stable adapter-owned contract, distinct from untrusted report data.
    system = (
        messages.system_message
        + "\n\n"
        + messages.task_message
        + "\n\nReturn exactly one JSON object matching this schema:\n"
        + json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    body: dict[str, Any] = {
        "model": route.model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": messages.data_preamble + "\n\n" + messages.data_json},
        ],
        "response_format": {"type": "json_object"},
        "max_tokens": route.max_output_tokens,
        "thinking": {"type": route.thinking},
        "request_id": invocation,
    }
    if route.thinking == "enabled":
        body["reasoning_effort"] = route.reasoning_effort
    for name in ("temperature", "top_p"):
        value = getattr(route, name)
        if value is not None:
            body[name] = value
    return body


def parse_chat(raw: bytes, route: ModelRoute, invocation: str, latency: int) -> ModelResponse:
    try:
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
        if data.get("error"):
            raise ProviderFailure("provider_response_failed")
        if not isinstance(data.get("id"), str) or not data["id"].strip():
            raise ProviderFailure("provider_response_id_missing")
        if data.get("model") != route.model:
            raise ProviderFailure("provider_model_mismatch")
        choices = data["choices"]
        if not isinstance(choices, list) or len(choices) != 1:
            raise ProviderFailure("provider_output_cardinality_invalid")
        choice = choices[0]
        reason = choice["finish_reason"]
        if reason == "length":
            raise ProviderFailure("provider_output_token_limit")
        if reason in ("sensitive", "content_filter") or choice["message"].get("refusal"):
            raise ProviderFailure("provider_refusal")
        if reason != "stop" or choice["message"].get("tool_calls"):
            raise ProviderFailure("provider_response_status_invalid")
        output = choice["message"]["content"]
        if not isinstance(output, str) or not output.strip():
            raise ProviderFailure("provider_output_cardinality_invalid")
        parsed = json.loads(output, object_pairs_hook=_pairs, parse_constant=_constant)
        if not isinstance(parsed, dict):
            raise ProviderFailure("provider_response_invalid")
        counts = [None, None]
        usage = data.get("usage")
        if usage is not None:
            counts = [usage.get("prompt_tokens"), usage.get("completion_tokens")]
            if any(v is not None and (type(v) is not int or v < 0) for v in counts):
                raise ProviderFailure("provider_usage_invalid")
        return ModelResponse(
            invocation,
            data["id"],
            data["model"],
            output,
            output,
            "unchanged",
            counts[0],
            counts[1],
            latency,
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ProviderFailure("provider_response_invalid") from None


class ZhipuChatModel(QSResponsesModel):
    @property
    def _llm_type(self) -> str:
        return "qs-zhipu-chat"

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        if (
            stop is not None
            or len(messages) != 2
            or not isinstance(messages[0], SystemMessage)
            or not isinstance(messages[1], HumanMessage)
            or any(not isinstance(m.content, str) for m in messages)
        ):
            raise ValueError("Unsupported GLM message contract")
        body = dict(kwargs["body"])
        body["messages"] = [
            {"role": role, "content": m.content}
            for role, m in zip(("system", "user"), messages, strict=True)
        ]
        receipt = await self._send(body, kwargs["route"], kwargs["invocation_id"])
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(
                        content=receipt.raw_output,
                        response_metadata={"qs_receipt": asdict(receipt)},
                    )
                )
            ]
        )

    def _parse_response(
        self, raw: bytes, route: ModelRoute, invocation_id: str, latency: int
    ) -> ModelResponse:
        return parse_chat(raw, route, invocation_id, latency)
