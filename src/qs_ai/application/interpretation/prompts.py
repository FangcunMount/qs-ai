"""Render trusted Prompt policy separately from untrusted assessment facts.

RenderPolicy is a projection of an already validated published Profile. This
module does not resolve Profiles, assemble evidence or validate model output.
"""

import json
import re
from dataclasses import dataclass


class InvalidPrompt(ValueError):
    pass


@dataclass(frozen=True)
class PromptPackage:
    template_id: str
    version: str
    fingerprint: str
    git_blob_sha: str | None
    system_message: str
    task_template: str
    data_preamble: str
    allowed_placeholders: tuple[str, ...]


@dataclass(frozen=True)
class RenderPolicy:
    template_id: str
    version: str
    allowed_focus_areas: tuple[str, ...]
    allowed_insight_kinds: tuple[str, ...]
    insight_min_items: int
    insight_max_items: int
    min_dimension_refs: int
    max_dimension_refs: int
    allow_parent_child_in_same_insight: bool
    allowed_suggestion_origins: tuple[str, ...]
    allowed_suggestion_categories: tuple[str, ...]
    suggestion_min_items: int
    suggestion_max_items: int
    max_actions_per_item: int
    max_output_characters: int


@dataclass(frozen=True)
class PromptMessages:
    system_message: str
    task_message: str
    data_preamble: str
    data_json: str


PLACEHOLDER = re.compile(r"\{\{[a-z0-9_]+\}\}")
CODE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}")
LOCALE = re.compile(r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*")


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidPrompt("Duplicate data field")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise InvalidPrompt("Non-JSON numeric constant")


def render_prompt(
    package: PromptPackage, policy: RenderPolicy, provider_payload: str
) -> PromptMessages:
    if (package.template_id, package.version) != (policy.template_id, policy.version):
        raise InvalidPrompt("Prompt and Profile identity mismatch")
    if not all(
        x.strip()
        for x in (
            package.system_message,
            package.task_template,
            package.data_preamble,
        )
    ):
        raise InvalidPrompt("Prompt messages are required")
    if "{{" in package.system_message or "{{" in package.data_preamble:
        raise InvalidPrompt("Dynamic system or data preamble is forbidden")
    allowed = package.allowed_placeholders
    if len(set(allowed)) != len(allowed) or any(not PLACEHOLDER.fullmatch(x) for x in allowed):
        raise InvalidPrompt("Invalid or duplicated placeholder")

    try:
        data = json.loads(
            provider_payload, object_pairs_hook=_unique_object, parse_constant=_invalid_constant
        )
    except (ValueError, TypeError):
        raise InvalidPrompt("Invalid provider payload") from None
    if not isinstance(data, dict) or set(data) != {"context", "facts"}:
        raise InvalidPrompt("Provider payload must contain context and facts only")
    context = data["context"]
    if not isinstance(context, dict) or not isinstance(data["facts"], dict):
        raise InvalidPrompt("Context and facts must be objects")
    locale, focus = context.get("locale"), context.get("focus_areas")
    if not isinstance(locale, str) or len(locale) > 35 or not LOCALE.fullmatch(locale):
        raise InvalidPrompt("Invalid locale")
    if not isinstance(focus, list) or any(not isinstance(x, str) for x in focus):
        raise InvalidPrompt("Invalid focus areas")
    if len(set(focus)) != len(focus) or any(x not in policy.allowed_focus_areas for x in focus):
        raise InvalidPrompt("Focus areas must be unique and allowed by Profile")

    codes = (
        policy.allowed_focus_areas,
        policy.allowed_insight_kinds,
        policy.allowed_suggestion_origins,
        policy.allowed_suggestion_categories,
    )
    if any(len(set(xs)) != len(xs) or any(not CODE.fullmatch(x) for x in xs) for xs in codes):
        raise InvalidPrompt("Invalid controlled policy codes")
    if not all(codes[1:]):
        raise InvalidPrompt("Policy kinds, origins and categories are required")
    ranges = (
        (policy.insight_min_items, policy.insight_max_items, 1, 8),
        (policy.min_dimension_refs, policy.max_dimension_refs, 2, 6),
        (policy.suggestion_min_items, policy.suggestion_max_items, 1, 8),
        (policy.max_actions_per_item, policy.max_actions_per_item, 1, 5),
        (policy.max_output_characters, policy.max_output_characters, 512, 20000),
    )
    if any(
        type(lo) is not int or type(hi) is not int or not floor <= lo <= hi <= ceiling
        for lo, hi, floor, ceiling in ranges
    ):
        raise InvalidPrompt("Invalid policy bounds")
    if type(policy.allow_parent_child_in_same_insight) is not bool:
        raise InvalidPrompt("Invalid hierarchy policy")

    values = {
        "locale": locale,
        "focus_areas_json": _json(focus),
        "allowed_insight_kinds_json": _json(policy.allowed_insight_kinds),
        "insight_min_items": str(policy.insight_min_items),
        "insight_max_items": str(policy.insight_max_items),
        "min_dimension_refs": str(policy.min_dimension_refs),
        "max_dimension_refs": str(policy.max_dimension_refs),
        "allow_parent_child_in_same_insight": _json(policy.allow_parent_child_in_same_insight),
        "allowed_suggestion_origins_json": _json(policy.allowed_suggestion_origins),
        "allowed_suggestion_categories_json": _json(policy.allowed_suggestion_categories),
        "suggestion_min_items": str(policy.suggestion_min_items),
        "suggestion_max_items": str(policy.suggestion_max_items),
        "max_actions_per_item": str(policy.max_actions_per_item),
        "max_output_characters": str(policy.max_output_characters),
    }
    for placeholder in PLACEHOLDER.findall(package.task_template):
        if placeholder not in allowed or placeholder[2:-2] not in values:
            raise InvalidPrompt("Unknown or forbidden placeholder")
    task = PLACEHOLDER.sub(lambda match: values[match[0][2:-2]], package.task_template)
    if "{{" in task or "}}" in task:
        raise InvalidPrompt("Unresolved placeholder")
    return PromptMessages(package.system_message, task, package.data_preamble, _json(data))
