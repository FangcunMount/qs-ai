"""Explicit executable identity; legacy route serialization remains untouched."""

import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Literal

from qs_ai.application.interpretation.provider import ModelRoute


@dataclass(frozen=True, kw_only=True)
class ModelRouteV2(ModelRoute):
    model_key: str
    catalog_revision: str
    binding_id: str
    binding_revision: str
    adapter_contract: str
    thinking: Literal["enabled", "disabled"] | None
    temperature: float | None
    top_p: float | None
    format_version: Literal["qs-ai-model-route/v2"] = "qs-ai-model-route/v2"

    def __post_init__(self) -> None:
        if self.format_version != "qs-ai-model-route/v2":
            raise ValueError("Unsupported model route version")
        for value in (
            self.route,
            self.revision,
            self.model,
            self.model_key,
            self.catalog_revision,
            self.binding_id,
            self.binding_revision,
        ):
            if not isinstance(value, str) or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", value
            ):
                raise ValueError("Invalid frozen model identity")
        expected = {
            "deepseek": ("responses", "json_schema", "deepseek-responses/v1"),
            "zhipu": ("chat_completions", "json_object", "zhipu-chat/v1"),
        }
        if expected.get(self.provider) != (
            self.protocol,
            self.structured_output_mode,
            self.adapter_contract,
        ):
            raise ValueError("Unsupported model adapter contract")
        if self.idempotent_redispatch is not False or self.retrieve_by_invocation_id is not False:
            raise ValueError("Model replay and retrieval are unsupported")
        for limit, lower, upper in (
            (self.timeout_milliseconds, 1000, 180000),
            (self.max_output_tokens, 1, 12000),
        ):
            if type(limit) is not int or not lower <= limit <= upper:
                raise ValueError("Invalid execution limit")
        if self.reasoning_effort not in {"none", "low", "high", "max"}:
            raise ValueError("Invalid reasoning effort")
        if self.thinking not in (None, "enabled", "disabled"):
            raise ValueError("Invalid thinking mode")
        if self.provider == "deepseek" and self.thinking is not None:
            raise ValueError("DeepSeek uses reasoning effort")
        if self.provider == "zhipu" and (
            self.thinking is None
            or (self.thinking == "enabled" and self.reasoning_effort == "none")
            or (self.thinking == "disabled" and self.reasoning_effort != "none")
        ):
            raise ValueError("Inconsistent thinking parameters")
        for sampling, minimum, maximum in (
            (self.temperature, 0, 2 if self.provider == "deepseek" else 1),
            (self.top_p, 0.01, 1),
        ):
            if sampling is not None and (
                type(sampling) not in (int, float)
                or not math.isfinite(sampling)
                or not minimum <= sampling <= maximum
            ):
                raise ValueError("Invalid sampling parameter")
        if self.temperature is not None and self.top_p is not None:
            raise ValueError("Choose one sampling parameter")
        if self.provider == "deepseek":
            if self.temperature is not None and self.reasoning_effort != "none":
                raise ValueError("Temperature is inactive in thinking mode")
            if self.top_p is not None and (self.reasoning_effort == "none" or self.top_p < 0.95):
                raise ValueError("Top p would be ignored or clamped")

    def definition_json(self) -> str:
        value = asdict(self)
        value["structured_output"] = True
        return json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
