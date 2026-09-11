"""Reviewed DeepSeek response wrappers; never repair JSON or business fields."""

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NormalizedOutput:
    raw_output: str
    validation_output: str
    normalization: str


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise ValueError("Non-JSON constant")


def _object(raw: str) -> dict[str, Any] | None:
    try:
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant)
        return value if isinstance(value, dict) else None
    except (TypeError, ValueError):
        return None


def _inner_object(raw: str) -> str:
    # The object has already passed unique-key JSON validation. Preserve the
    # exact enclosed bytes so whitespace still counts toward output limits.
    decoder = json.JSONDecoder()
    key_start = len(raw) - len(raw[1:].lstrip())
    _, key_end = decoder.raw_decode(raw, key_start)
    value_start = raw.index(":", key_end) + 1
    value_start += len(raw[value_start:]) - len(raw[value_start:].lstrip())
    _, value_end = decoder.raw_decode(raw, value_start)
    return raw[value_start:value_end]


def normalize_output(provider: str, raw: str) -> NormalizedOutput:
    unchanged = NormalizedOutput(raw, raw, "unchanged")
    if provider != "deepseek":
        return unchanged
    candidate = raw.strip()
    kind = "unchanged"
    opening = candidate.find("\n")
    closing = candidate.rfind("\n```")
    if opening >= 0 and candidate[:opening].strip().lower() == "```json":
        if closing > opening and candidate[closing + 1 :].strip() == "```":
            enclosed = candidate[opening + 1 : closing].strip()
            if _object(enclosed) is not None:
                candidate, kind = enclosed, "markdown_unwrapped"
    envelope = _object(candidate)
    if envelope is not None and len(envelope) == 1:
        for key in ("parameters", "json", "json_string"):
            if key not in envelope:
                continue
            value = envelope[key]
            if key == "json_string":
                if isinstance(value, str) and _object(value.strip()) is not None:
                    return NormalizedOutput(raw, value.strip(), "envelope_unwrapped")
            elif isinstance(value, dict):
                return NormalizedOutput(
                    raw,
                    _inner_object(candidate),
                    "envelope_unwrapped",
                )
    if kind != "unchanged":
        return NormalizedOutput(raw, candidate, kind)
    return unchanged
