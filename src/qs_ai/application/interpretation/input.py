"""Project frozen QS report facts into the existing AIExplanationInput v1.

The caller supplies policy from a validated published Profile. Authorization and
Profile publication are separate use cases; neither is inferred from report JSON.
"""

import hashlib
import json
import re
from typing import Any

from qs_ai.application.interpretation.input_values import (
    AssembledInput as AssembledInput,
)
from qs_ai.application.interpretation.input_values import (
    InputPolicy as InputPolicy,
)
from qs_ai.application.interpretation.input_values import (
    InvalidInput as InvalidInput,
)
from qs_ai.application.interpretation.input_values import (
    MBTIInputPolicy as MBTIInputPolicy,
)
from qs_ai.application.interpretation.input_values import (
    NotApplicable as NotApplicable,
)
from qs_ai.application.interpretation.input_values import (
    _json as _json,
)
from qs_ai.application.interpretation.input_values import (
    _number as _number,
)
from qs_ai.application.interpretation.input_values import (
    _object as _object,
)
from qs_ai.application.interpretation.input_values import (
    _plain as _plain,
)
from qs_ai.application.interpretation.input_values import (
    _ref as _ref,
)
from qs_ai.application.interpretation.input_values import (
    _suggestions as _suggestions,
)


def _score(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "kind": value["kind"],
        "value": _number(value["value"]),
        "label": value["label"],
        "max": _number(value["max"], True),
    }


def _level(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    result = {key: value[key] for key in ("code", "label", "severity")}
    # QS can persist a standard level code without a display label. Preserve that
    # exact code as its label; do not infer a translation or risk severity.
    if result["label"] == "":
        result["label"] = _plain(result["code"], 2000)
    return result


def _norm(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    result = {
        key: value[key]
        for key in (
            "score_kind",
            "benchmark",
            "table_version",
            "form_variant",
            "min_age_months",
            "max_age_months",
            "gender",
        )
    }
    _number(result["benchmark"])
    return result


def assemble_input(
    raw_snapshot: str,
    policy: InputPolicy,
    *,
    locale: str = "zh-CN",
    focus_areas: tuple[str, ...] = (),
) -> AssembledInput:
    try:
        snapshot = json.loads(raw_snapshot, object_pairs_hook=_object)
        if isinstance(policy, MBTIInputPolicy):
            from qs_ai.application.interpretation.mbti_input import assemble_mbti

            return assemble_mbti(snapshot, policy, locale, focus_areas)
        return _assemble(snapshot, policy, locale, focus_areas)
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        if isinstance(error, InvalidInput):
            raise
        # Never include participant data from decoding/conversion exceptions.
        raise InvalidInput("Invalid report snapshot") from None


def _assemble(
    snapshot: dict[str, Any],
    policy: InputPolicy,
    locale: str,
    focus: tuple[str, ...],
) -> AssembledInput:
    if snapshot["schema_version"] != "qs-report-snapshot/v1":
        raise InvalidInput("Unsupported report snapshot version")
    if (
        not policy.profile_id
        or not policy.profile_version
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", policy.profile_fingerprint)
    ):
        raise InvalidInput("Published Profile reference is required")
    if not 2 <= policy.min_dimensions <= policy.max_dimensions <= 50:
        raise InvalidInput("Invalid eligibility bounds")
    if set(policy.eligible_codes) & set(policy.excluded_codes):
        raise InvalidInput("Overlapping eligibility policy")
    if not re.fullmatch(r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*", locale) or len(locale) > 35:
        raise InvalidInput("Invalid locale")
    if len(set(focus)) != len(focus) or any(
        not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", x) or x not in policy.allowed_focus_areas
        for x in focus
    ):
        raise InvalidInput("Invalid or disallowed focus areas")
    model, runtime = snapshot["model"], snapshot["runtime"]
    if model["kind"] != "scale" or runtime["decision_kind"] != "score_range":
        raise NotApplicable("Input v1 requires participant scale/score_range")
    if (policy.model_code is not None and model["code"] != policy.model_code) or (
        policy.model_version is not None and model["version"] != policy.model_version
    ):
        raise NotApplicable("Profile selector does not match report")
    for key in ("algorithm", "code", "version", "title"):
        _plain(model[key], 2000)

    source = snapshot["source"]
    for key in ("report_id", "outcome_id"):
        if not re.fullmatch(r"[1-9][0-9]{0,19}", source[key]) or int(source[key]) >= 2**64:
            raise InvalidInput("Invalid source identity")
    for key in (
        "report_template_version",
        "content_schema_version",
        "builder_identity",
        "generated_at",
    ):
        _plain(source[key], 255)
    if source["report_type"] != "standard":
        raise NotApplicable("Only standard reports are accepted")

    dimensions = snapshot["dimensions"]
    codes = [d["code"] for d in dimensions]
    if any(not isinstance(code, str) or not code for code in codes) or len(set(codes)) != len(
        codes
    ):
        raise InvalidInput("Missing or duplicate dimension code")
    selected = [
        d
        for d in dimensions
        if (not policy.eligible_codes or d["code"] in policy.eligible_codes)
        and d["code"] not in policy.excluded_codes
    ]
    if not policy.min_dimensions <= len(selected) <= policy.max_dimensions:
        raise NotApplicable("Eligible dimension count outside Profile bounds")
    refs = {d["code"]: "dimension:" + _ref(d["code"]) for d in selected}
    suggestions, by_dimension = _suggestions(snapshot, refs)
    projected = []
    for d in selected:
        for field in ("hierarchy_level", "sort_order"):
            if type(d[field]) is not int or d[field] < 0:
                raise InvalidInput("Invalid dimension hierarchy or sort order")
        projected.append(
            {
                "ref": refs[d["code"]],
                "code": d["code"],
                "kind": _plain(d["kind"], 255),
                "name": _plain(d["name"], 2000),
                "role": d["role"],
                "parent_ref": refs.get(d["parent_code"]),
                "hierarchy_level": d["hierarchy_level"],
                "sort_order": d["sort_order"],
                "raw_score": {
                    "kind": "raw_score",
                    "value": _number(d["raw_score"]),
                    "label": "",
                    "max": _number(d["max_score"], True),
                },
                "derived_scores": [_score(score) for score in d["derived_scores"]],
                "level": _level(d["level"]),
                "norm_context": _norm(d["norm_reference"]) if policy.include_norm_context else None,
                "standard_description": _plain(d["description"], 4000, False),
                "standard_suggestion_refs": by_dimension.get(d["code"], []),
            }
        )
    projected.sort(key=lambda d: (d["sort_order"], d["code"]))
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
        "runtime": {"decision_kind": runtime["decision_kind"]},
        "model": {key: model[key] for key in ("kind", "algorithm", "code", "version", "title")},
        "overall_result": {
            "primary_score": _score(snapshot["primary_score"]),
            "level": _level(snapshot["level"]),
            "standard_conclusion": snapshot["conclusion"],
        },
        "dimensions": projected,
        "standard_suggestions": suggestions,
        "model_result": _model_result(snapshot["model_extra"])
        if policy.include_model_result
        else None,
    }
    document = {
        "schema_version": "ai-explanation-input/v1",
        "source": {
            key: source[key]
            for key in (
                "report_id",
                "outcome_id",
                "report_type",
                "report_template_version",
                "content_schema_version",
                "builder_identity",
                "generated_at",
            )
        },
        "profile": {
            "profile_id": policy.profile_id,
            "profile_version": policy.profile_version,
            "profile_fingerprint": policy.profile_fingerprint,
        },
        "context": context,
        "facts": facts,
    }
    canonical = _json(document)
    return AssembledInput(
        canonical,
        "sha256:" + hashlib.sha256(canonical.encode()).hexdigest(),
        _json({"context": context, "facts": facts}),
    )


def _model_result(extra: dict[str, Any] | None) -> dict[str, Any] | None:
    if extra is None:
        return None
    for key in ("kind", "type_code", "type_name", "one_liner"):
        _plain(extra.get(key), 2000)
    percent = extra.get("match_percent", 0)
    if type(percent) not in (int, float) or not 0 <= percent <= 100:
        raise InvalidInput("Invalid model match percent")
    return {
        "kind": extra["kind"],
        "type_code": extra["type_code"],
        "type_name": extra["type_name"],
        "one_liner": extra["one_liner"],
        "match_percent": percent,
        "commentary": extra.get("commentary", ""),
    }
