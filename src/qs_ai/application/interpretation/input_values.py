"""Framework-free input values and shared field projection helpers."""

import json
import math
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote


class InvalidInput(ValueError):
    pass


class NotApplicable(InvalidInput):
    pass


@dataclass(frozen=True)
class InputPolicy:
    profile_id: str
    profile_version: str
    profile_fingerprint: str
    model_code: str | None
    model_version: str | None
    min_dimensions: int
    max_dimensions: int
    eligible_codes: tuple[str, ...]
    excluded_codes: tuple[str, ...]
    allowed_focus_areas: tuple[str, ...]
    include_norm_context: bool
    include_model_result: bool


@dataclass(frozen=True)
class MBTIInputPolicy(InputPolicy):
    scene_contract_version: str


@dataclass(frozen=True)
class AssembledInput:
    canonical_json: str
    fingerprint: str
    provider_payload: str


def _plain(value: Any, limit: int, required: bool = True) -> str:
    if not isinstance(value, str) or len(value) > limit or (required and not value):
        raise InvalidInput("Invalid report text")
    if "<" in value or ">" in value:
        raise InvalidInput("Report text must be plain text")
    return value


def _ref(code: str) -> str:
    # Go url.PathEscape leaves these path-segment characters unescaped.
    return quote(code, safe="$&+.:=@_")


def _number(value: Any, optional: bool = False) -> int | float | None:
    if optional and value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise InvalidInput("Invalid numeric report fact")
    return value


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise InvalidInput("Duplicate snapshot field")
        value[key] = item
    return value


def _suggestions(
    snapshot: dict[str, Any], refs: dict[str, str]
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    result: list[dict[str, Any]] = []
    by_dimension: dict[str, list[str]] = {}
    seen: set[tuple[str, str, str]] = set()

    def append(ref: str, category: str, text: str, code: str) -> None:
        if not category:
            raise InvalidInput("Suggestion category is required")
        _plain(text, 2000)
        key = (category, text, code)
        if key in seen:
            return
        seen.add(key)
        if code and code not in refs:
            return
        dimension_refs = [refs[code]] if code else []
        if code:
            by_dimension.setdefault(code, []).append(ref)
        result.append(
            {"ref": ref, "category": category, "content": text, "dimension_refs": dimension_refs}
        )

    indices = set()
    for suggestion in snapshot["suggestions"]:
        index = suggestion["source_index"]
        if type(index) is not int or index < 0 or index in indices:
            raise InvalidInput("Invalid original suggestion index")
        indices.add(index)
        append(
            f"suggestion:report:{index + 1}",
            suggestion["category"],
            suggestion["content"],
            suggestion["dimension_code"] or "",
        )
    for dimension in snapshot["dimensions"]:
        if dimension["suggestion"].strip():
            append(
                "suggestion:dimension:" + _ref(dimension["code"]),
                "dimension",
                dimension["suggestion"],
                dimension["code"],
            )
    return result, by_dimension
