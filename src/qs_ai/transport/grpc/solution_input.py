"""Strict wire parsing stays outside application and domain layers."""

import json
from typing import Any
from uuid import UUID

from qs_ai.application.governance.solutions import (
    CreateSolution,
    ModelSelection,
    PrepareSolution,
    SaveSolution,
)
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.prompt_draft import PromptDraftContent


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate field")
        result[key] = value
    return result


def parse_command(
    raw: str, model: type[CreateSolution] | type[SaveSolution] | type[PrepareSolution]
) -> CreateSolution | SaveSolution | PrepareSolution:
    value = json.loads(raw, object_pairs_hook=unique_object)
    if not isinstance(value, dict):
        raise ValueError("Command object required")
    for key in ("command_id", "publication_id", "source_run_id"):
        if key in value and value[key] is not None:
            original = value[key]
            if not isinstance(original, str):
                raise ValueError("String identity required")
            value[key] = UUID(original)
            if str(value[key]) != original:
                raise ValueError("Canonical identity required")
    if model is CreateSolution and value.get("template_ref") is not None:
        value["template_ref"] = FrozenContractRef(**value["template_ref"])
    if model is SaveSolution:
        for key in ("evaluation_suite", "semantic_prompt"):
            if value.get(key) is not None:
                value[key] = FrozenContractRef(**value[key])
        for key in ("generation", "semantic"):
            value[key] = ModelSelection(**value[key])
        content = dict(value["content"])
        if not isinstance(content.get("allowed_placeholders"), list):
            raise ValueError("Placeholders array required")
        content["allowed_placeholders"] = tuple(content["allowed_placeholders"])
        value["content"] = PromptDraftContent(**content)
    return model(**value)
