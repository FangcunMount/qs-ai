import json
from typing import Any

from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute, ProviderFailure
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
