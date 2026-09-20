"""Constrained revisions keep case identities, preflight and hard assertions intact."""

import copy
import json
from typing import Any

# These assertions enforce safety rather than an editable quality preference.
HARD_ASSERTIONS = frozenset(
    {
        "forbid_identity_essentialism",
        "no_risk_escalation",
        "norm_claims_match_input",
        "no_standard_derived_without_sources",
        "no_unprovided_fact",
        "uncertainty_matches_evidence",
        "focus_area_not_treated_as_fact",
        "ignore_embedded_instruction",
        "forbid_source_suggestion_ref",
        "forbid_literal_substrings",
        "forbid_dimension_group",
    }
)


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate case field")
        result[key] = value
    return result


def parse(raw: str) -> list[dict[str, Any]]:
    if not isinstance(raw, str) or len(raw.encode()) > 8192:
        raise ValueError("Case edits exceed limit")
    if not raw:
        return []

    def invalid_constant(value: str) -> None:
        raise ValueError("Non-finite case value")

    values = json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    if not isinstance(values, list) or not 1 <= len(values) <= 7:
        raise ValueError("One to seven explicit case revisions required")
    ids = set()
    for value in values:
        if not isinstance(value, dict) or set(value) != {
            "case_id",
            "title",
            "purpose",
            "provider_payload",
            "assertions",
        }:
            raise ValueError("Complete case revision required")
        for field in ("case_id", "title", "purpose"):
            text = value[field]
            if not isinstance(text, str) or not text.strip() or len(text) > 512 or "\0" in text:
                raise ValueError("Bounded case identity and description required")
        if value["case_id"] in ids:
            raise ValueError("Duplicate case revision")
        ids.add(value["case_id"])
        if not isinstance(value["provider_payload"], dict) or not isinstance(
            value["assertions"], list
        ):
            raise ValueError("Structured input and assertions required")
    return values


def compatible(original: Any, updated: Any) -> bool:
    if type(updated) is not type(original):
        return False
    if isinstance(original, str):
        return bool(updated.strip()) and len(updated) <= 512 and "\0" not in updated
    if isinstance(original, list):
        return 1 <= len(updated) <= 32 and all(
            isinstance(item, str) and bool(item.strip()) and len(item) <= 512 for item in updated
        )
    if type(original) is int:
        return 0 <= updated <= 8000
    return original == updated


def apply(cases: list[dict[str, Any]], raw: str) -> list[dict[str, Any]]:
    result = copy.deepcopy(cases)
    for edit in parse(raw):
        case = next((item for item in result if item["case_id"] == edit["case_id"]), None)
        if case is None or case["stage"] != "generation":
            raise ValueError("Only existing generation cases may be revised")
        previous, updated = case["expected"]["assertions"], edit["assertions"]
        if len(previous) != len(updated):
            raise ValueError("Case assertion obligations cannot be removed or reordered")
        for before, after in zip(previous, updated, strict=True):
            if (
                not isinstance(after, dict)
                or before.keys() != after.keys()
                or before["type"] != after.get("type")
            ):
                raise ValueError("Assertion types and parameter contracts are immutable")
            if before["type"] in HARD_ASSERTIONS and before != after:
                raise ValueError("Core safety assertions cannot be edited")
            if any(not compatible(value, after[key]) for key, value in before.items()):
                raise ValueError("Assertion parameter invalid")
        case.update({key: edit[key] for key in ("title", "purpose", "provider_payload")})
        case["expected"]["assertions"] = updated
    return result
