"""QS-compatible DeepSeek Responses request projection, with no network I/O."""

import re
from typing import Any

from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ModelRoute


def compatible_schema(schema: dict[str, Any]) -> dict[str, Any]:
    definitions = schema.get("$defs", {})

    def inferred(value: Any) -> str:
        return {str: "string", bool: "boolean", int: "number", float: "number"}.get(type(value), "")

    def transform(node: dict[str, Any], visiting: frozenset[str]) -> dict[str, Any]:
        if "$ref" in node:
            ref = node["$ref"]
            if not ref.startswith("#/$defs/") or ref in visiting or ref[8:] not in definitions:
                raise ValueError("Unsupported or cyclic schema reference")
            return transform(definitions[ref[8:]], visiting | {ref})
        result: dict[str, Any] = {}
        if node.get("description", "").strip():
            result["description"] = node["description"]
        if isinstance(node.get("type"), str):
            result["type"] = node["type"]
        if "enum" in node:
            values = node["enum"]
            result["enum"] = list(values)
            types = {inferred(value) for value in values}
            if "type" not in result and len(types) == 1 and "" not in types:
                result["type"] = next(iter(types))
        if "const" in node:
            result["enum"] = [node["const"]]
            if inferred(node["const"]):
                result["type"] = inferred(node["const"])
        if "properties" in node:
            result.update(
                type="object",
                properties={
                    key: transform(value, visiting) for key, value in node["properties"].items()
                },
                required=sorted(node["properties"]),
                additionalProperties=False,
            )
        if "items" in node:
            result.update(type="array", items=transform(node["items"], visiting))
        if "anyOf" in node:
            result["anyOf"] = [transform(value, visiting) for value in node["anyOf"]]
        if not result:
            raise ValueError("Schema has no supported constraints")
        return result

    return transform(schema, frozenset())


def build_request(
    prepared: PreparedExplanation, route: ModelRoute, schema: dict[str, Any]
) -> dict[str, Any]:
    if route.route != prepared.release.provider_route:
        raise ValueError("Provider route does not match published Profile")
    return build_messages_request(prepared.messages, route, schema)


def build_messages_request(
    messages: PromptMessages, route: ModelRoute, schema: dict[str, Any]
) -> dict[str, Any]:
    """Serialize validated stage-specific messages; frozen asset checks belong to the caller."""
    if route.provider != "deepseek" or route.protocol != "responses":
        raise ValueError("This adapter requires DeepSeek Responses protocol")
    if route.idempotent_redispatch or route.retrieve_by_invocation_id:
        raise ValueError("This adapter does not support replay or invocation retrieval")
    if (
        not route.model.strip()
        or not route.revision.strip()
        or type(route.max_output_tokens) is not int
        or route.max_output_tokens < 1
        or type(route.timeout_milliseconds) is not int
        or route.timeout_milliseconds < 1
    ):
        raise ValueError("Incomplete execution route")
    if route.reasoning_effort not in {
        "",
        "none",
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    }:
        raise ValueError("Invalid reasoning effort")
    if route.structured_output_mode == "json_schema":
        name = re.sub(r"[^a-zA-Z0-9_-]+", "_", schema["title"].strip()).strip("_")[:64]
        format_value = {
            "type": "json_schema",
            "name": name or "ai_explanation_output",
            "schema": compatible_schema(schema),
        }
    elif route.structured_output_mode == "json_object":
        if "json" not in (messages.system_message + messages.task_message).lower():
            raise ValueError("JSON instruction is required")
        format_value = {"type": "json_object"}
    else:
        raise ValueError("Unsupported structured output mode")
    result: dict[str, Any] = {
        "model": route.model,
        "instructions": messages.system_message,
        "input": [
            {
                "role": "developer",
                "content": [{"type": "input_text", "text": messages.task_message}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": messages.data_preamble + "\n\n" + messages.data_json,
                    }
                ],
            },
        ],
        "text": {"format": format_value},
        "max_output_tokens": route.max_output_tokens,
    }
    if route.reasoning_effort:
        result["reasoning"] = {"effort": route.reasoning_effort}
    if isinstance(route, ModelRouteV2):
        for name in ("temperature", "top_p"):
            value = getattr(route, name)
            if value is not None:
                result[name] = value
    # QS omits strict and store for this provider. Local full validation remains mandatory.
    return result
