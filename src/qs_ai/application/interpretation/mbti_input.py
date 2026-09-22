"""Strict MBTI snapshot projection. QS owns scoring; no scoring is performed here.

Only typed frozen report facts are accepted. No model lookup, description parsing,
default strengths, or translation of preference strength into confidence.
"""

import hashlib
import re
from copy import deepcopy
from datetime import datetime
from typing import Any

from qs_ai.application.interpretation.input_values import (
    AssembledInput,
    InvalidInput,
    MBTIInputPolicy,
    _json,
    _number,
    _plain,
    _suggestions,
)
from qs_ai.domain.governance.scenes import (
    MBTI_AXES,
    MBTI_CONTRACT,
    MBTI_MODEL,
    MBTI_VERSION,
)


def _fields(value: Any, fields: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields.split()):
        raise ValueError("Invalid contract fields")
    return value


def _bounded(value: Any, minimum: float, maximum: float) -> None:
    number = _number(value)
    if number is None or not minimum <= number <= maximum:
        raise ValueError("Invalid contract number")


def decode_mbti_snapshot(value: Any) -> dict[str, Any]:
    try:
        return _validate_snapshot(value)
    except (ValueError, TypeError, KeyError, IndexError, AttributeError):
        raise InvalidInput("Invalid MBTI report snapshot") from None


def _validate_snapshot(value: Any) -> dict[str, Any]:
    s = _fields(
        value, "schema_version source model runtime conclusion dimensions suggestions model_extra"
    )
    if s["schema_version"] != "qs-report-snapshot/v2":
        raise ValueError("Unsupported MBTI snapshot")
    source = _fields(
        s["source"],
        "report_id outcome_id report_type report_template_version "
        "content_schema_version builder_identity generated_at",
    )
    for key in ("report_id", "outcome_id"):
        if (
            not isinstance(source[key], str)
            or not re.fullmatch(r"[1-9][0-9]{0,19}", source[key])
            or int(source[key]) >= 2**64
        ):
            raise ValueError("Invalid source identity")
    for key in (
        "report_template_version",
        "content_schema_version",
        "builder_identity",
        "generated_at",
    ):
        _plain(source[key], 255)
    if (
        source["report_type"] != "standard"
        or datetime.fromisoformat(source["generated_at"].replace("Z", "+00:00")).tzinfo is None
    ):
        raise ValueError("Invalid report source")
    model = _fields(s["model"], "kind algorithm code version title")
    if tuple(model[k] for k in ("kind", "algorithm", "code", "version")) != (
        "typology",
        "personality_typology",
        MBTI_MODEL,
        MBTI_VERSION,
    ):
        raise ValueError("Invalid MBTI model")
    _plain(model["title"], 2000)
    if _fields(s["runtime"], "decision_kind")["decision_kind"] != "pole_composition":
        raise ValueError("Invalid decision")
    _plain(s["conclusion"], 4000, False)
    result = _fields(
        s["model_extra"], "kind type_code type_name one_liner match_percent commentary"
    )
    if (
        result["kind"] != "personality_type"
        or not isinstance(result["type_code"], str)
        or not re.fullmatch(r"[IE][SN][FT][JP]", result["type_code"])
    ):
        raise ValueError("Invalid type")
    _plain(result["type_name"], 2000)
    for key in ("one_liner", "commentary"):
        _plain(result[key], 4000, False)
    _bounded(result["match_percent"], 0, 100)
    if not isinstance(s["dimensions"], list) or len(s["dimensions"]) != 4:
        raise ValueError("Incomplete MBTI axes")
    preferences: dict[int, str] = {}
    for d in s["dimensions"]:
        _fields(d, "code kind name raw_score pole_facts description suggestion")
        _plain(d["name"], 2000)
        for key in ("description", "suggestion"):
            _plain(d[key], 4000, False)
        _bounded(d["raw_score"], 8, 40)
        p = _fields(
            d["pole_facts"],
            "schema_version left_pole right_pole preference strength "
            "min_score max_score threshold composition_order",
        )
        order = p["composition_order"]
        if type(order) is not int or not 1 <= order <= 4 or order in preferences:
            raise ValueError("Duplicate or invalid axis order")
        if (
            d["kind"] != "pole"
            or p["schema_version"] != "mbti-pole-facts/v1"
            or (d["code"], p["left_pole"], p["right_pole"]) != MBTI_AXES[order - 1]
            or p["preference"] not in (p["left_pole"], p["right_pole"])
        ):
            raise ValueError("Invalid axis identity")
        for key, expected in (("min_score", 8), ("max_score", 40), ("threshold", 24)):
            _bounded(p[key], expected, expected)
        _bounded(p["strength"], 0, 100)
        preferences[order] = p["preference"]
    if "".join(preferences[i] for i in range(1, 5)) != result["type_code"]:
        raise ValueError("Type and preferences conflict")
    suggestions = s["suggestions"]
    if not isinstance(suggestions, list) or len(suggestions) > 50:
        raise ValueError("Invalid suggestions")
    indices = set()
    for suggestion in suggestions:
        _fields(suggestion, "source_index category content dimension_code")
        index = suggestion["source_index"]
        if type(index) is not int or index < 0 or index in indices:
            raise ValueError("Invalid suggestion index")
        indices.add(index)
        _plain(suggestion["category"], 2000)
        _plain(suggestion["content"], 2000)
        if suggestion["dimension_code"] not in (None, "EI", "SN", "TF", "JP"):
            raise ValueError("Invalid suggestion dimension")
    return deepcopy(s)


def assemble_mbti(
    value: Any, policy: MBTIInputPolicy, locale: str, focus: tuple[str, ...]
) -> AssembledInput:
    snapshot = decode_mbti_snapshot(value)
    if (
        policy.scene_contract_version != MBTI_CONTRACT
        or (policy.model_code, policy.model_version) != (MBTI_MODEL, MBTI_VERSION)
        or (policy.min_dimensions, policy.max_dimensions) != (4, 4)
        or policy.eligible_codes != tuple(axis[0] for axis in MBTI_AXES)
        or policy.excluded_codes
        or policy.include_norm_context
        or not policy.include_model_result
        or not policy.profile_id.strip()
        or not policy.profile_version.strip()
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", policy.profile_fingerprint)
    ):
        raise InvalidInput("Invalid MBTI input policy")
    if len(locale) > 35 or not re.fullmatch(r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*", locale):
        raise InvalidInput("Invalid locale")
    if (
        len(focus) > 3
        or len(set(focus)) != len(focus)
        or any(
            not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", item)
            or item not in policy.allowed_focus_areas
            for item in focus
        )
    ):
        raise InvalidInput("Invalid or disallowed focus areas")
    refs: dict[str, str] = {d["code"]: "dimension:" + d["code"] for d in snapshot["dimensions"]}
    normalized = snapshot
    normalized["dimensions"].sort(key=lambda d: d["pole_facts"]["composition_order"])
    normalized["suggestions"].sort(key=lambda s: s["source_index"])
    suggestions, by_dimension = _suggestions(normalized, refs)
    dimensions = []
    for d in sorted(snapshot["dimensions"], key=lambda d: d["pole_facts"]["composition_order"]):
        dimensions.append(
            {
                "ref": refs[d["code"]],
                "code": d["code"],
                "kind": d["kind"],
                "name": d["name"],
                "parent_ref": None,
                "raw_score": {
                    "kind": "raw_score",
                    "value": d["raw_score"],
                    "label": "",
                    "max": d["pole_facts"]["max_score"],
                },
                "pole_facts": d["pole_facts"],
                "strength_semantics": "preference_strength_not_confidence",
                "standard_description": d["description"],
                "standard_suggestion_refs": by_dimension.get(d["code"], []),
            }
        )
    context = {
        "scope": "current_assessment_only",
        "audience": "participant",
        "locale": locale,
        "personalization_scope": "assessment_result_and_focus_areas"
        if focus
        else "assessment_result_only",
        "focus_areas": list(focus),
    }
    facts = {
        "runtime": snapshot["runtime"],
        "model": snapshot["model"],
        "overall_result": {"standard_conclusion": snapshot["conclusion"]},
        "dimensions": dimensions,
        "standard_suggestions": suggestions,
        "model_result": snapshot["model_extra"],
    }
    canonical = _json(
        {
            "schema_version": "ai-explanation-input/v2",
            "scene_contract_version": policy.scene_contract_version,
            "source": snapshot["source"],
            "profile": {
                "profile_id": policy.profile_id,
                "profile_version": policy.profile_version,
                "profile_fingerprint": policy.profile_fingerprint,
            },
            "context": context,
            "facts": facts,
        }
    )
    return AssembledInput(
        canonical,
        "sha256:" + hashlib.sha256(canonical.encode()).hexdigest(),
        _json({"context": context, "facts": facts}),
    )
