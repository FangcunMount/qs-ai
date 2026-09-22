"""Strict MBTI snapshot projection. QS owns scoring; no scoring is performed here.

Only typed frozen report facts are accepted. No model lookup, description parsing,
default strengths, or translation of preference strength into confidence.
"""

import hashlib
import re
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qs_ai.application.interpretation.input import (
    AssembledInput,
    InvalidInput,
    MBTIInputPolicy,
    _json,
    _suggestions,
)
from qs_ai.domain.governance.scenes import (
    MBTI_AXES,
    MBTI_CONTRACT,
    MBTI_MODEL,
    MBTI_VERSION,
)

Text = Annotated[str, Field(min_length=1, max_length=2000, pattern=r"^[^<>]+$")]
Description = Annotated[str, Field(max_length=4000, pattern=r"^[^<>]*$")]
Number = Annotated[int | float, Field(allow_inf_nan=False)]
Identity = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,19}$")]
SourceVersion = Annotated[str, Field(min_length=1, max_length=255, pattern=r"^[^<>]+$")]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Source(Contract):
    report_id: Identity
    outcome_id: Identity
    report_type: Literal["standard"]
    report_template_version: SourceVersion
    content_schema_version: SourceVersion
    builder_identity: SourceVersion
    generated_at: SourceVersion

    @model_validator(mode="after")
    def identities(self) -> Self:
        if int(self.report_id) >= 2**64 or int(self.outcome_id) >= 2**64:
            raise ValueError("Invalid source identity")
        at = datetime.fromisoformat(self.generated_at.replace("Z", "+00:00"))
        if at.tzinfo is None:
            raise ValueError("Source timestamp requires timezone")
        return self


class Model(Contract):
    kind: Literal["typology"]
    algorithm: Literal["personality_typology"]
    code: Literal["MBTI_OEJTS"]
    version: Literal["v64-report-202608-v1"]
    title: Text


class Runtime(Contract):
    decision_kind: Literal["pole_composition"]


class PoleFacts(Contract):
    schema_version: Literal["mbti-pole-facts/v1"]
    left_pole: Text
    right_pole: Text
    preference: Text
    strength: Number = Field(ge=0, le=100)
    min_score: Number
    max_score: Number
    threshold: Number
    composition_order: int = Field(ge=1, le=4)


class Dimension(Contract):
    code: Literal["EI", "SN", "TF", "JP"]
    kind: Literal["pole"]
    name: Text
    raw_score: Number = Field(ge=8, le=40)
    pole_facts: PoleFacts
    description: Description
    suggestion: Description

    @model_validator(mode="after")
    def axis(self) -> Self:
        p = self.pole_facts
        if (
            (self.code, p.left_pole, p.right_pole) != MBTI_AXES[p.composition_order - 1]
            or p.preference not in (p.left_pole, p.right_pole)
            or (p.min_score, p.max_score, p.threshold) != (8, 40, 24)
        ):
            raise ValueError("MBTI axis does not match frozen model contract")
        return self


class ModelResult(Contract):
    kind: Literal["personality_type"]
    type_code: Annotated[str, Field(pattern=r"^[IE][SN][FT][JP]$")]
    type_name: Text
    one_liner: Description
    match_percent: Number = Field(ge=0, le=100)
    commentary: Description


class Suggestion(Contract):
    source_index: int = Field(ge=0)
    category: Text
    content: Annotated[str, Field(min_length=1, max_length=2000, pattern=r"^[^<>]+$")]
    dimension_code: Literal["EI", "SN", "TF", "JP"] | None


class MBTISnapshot(Contract):
    schema_version: Literal["qs-report-snapshot/v2"]
    source: Source
    model: Model
    runtime: Runtime
    conclusion: Description
    dimensions: list[Dimension] = Field(min_length=4, max_length=4)
    suggestions: list[Suggestion] = Field(max_length=50)
    type_result: ModelResult = Field(alias="model_extra")

    @model_validator(mode="after")
    def coherent_result(self) -> Self:
        ordered = sorted(self.dimensions, key=lambda d: d.pole_facts.composition_order)
        if tuple(d.code for d in ordered) != tuple(axis[0] for axis in MBTI_AXES):
            raise ValueError("Missing or repeated MBTI axis")
        if "".join(d.pole_facts.preference for d in ordered) != self.type_result.type_code:
            raise ValueError("Type and axis preferences conflict")
        indices = [s.source_index for s in self.suggestions]
        if len(indices) != len(set(indices)):
            raise ValueError("Duplicate suggestion source")
        return self


def decode_mbti_snapshot(value: Any) -> MBTISnapshot:
    try:
        return MBTISnapshot.model_validate(value)
    except ValueError:
        # ValidationError contains input values; never let it cross this boundary.
        raise InvalidInput("Invalid MBTI report snapshot") from None


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
    refs: dict[str, str] = {d.code: "dimension:" + d.code for d in snapshot.dimensions}
    normalized = snapshot.model_dump()
    normalized["dimensions"].sort(key=lambda d: d["pole_facts"]["composition_order"])
    normalized["suggestions"].sort(key=lambda s: s["source_index"])
    suggestions, by_dimension = _suggestions(normalized, refs)
    dimensions = []
    for d in sorted(snapshot.dimensions, key=lambda d: d.pole_facts.composition_order):
        dimensions.append(
            {
                "ref": refs[d.code],
                "code": d.code,
                "kind": d.kind,
                "name": d.name,
                "parent_ref": None,
                "raw_score": {
                    "kind": "raw_score",
                    "value": d.raw_score,
                    "label": "",
                    "max": d.pole_facts.max_score,
                },
                "pole_facts": d.pole_facts.model_dump(),
                "strength_semantics": "preference_strength_not_confidence",
                "standard_description": d.description,
                "standard_suggestion_refs": by_dimension.get(d.code, []),
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
        "runtime": snapshot.runtime.model_dump(),
        "model": snapshot.model.model_dump(),
        "overall_result": {"standard_conclusion": snapshot.conclusion},
        "dimensions": dimensions,
        "standard_suggestions": suggestions,
        "model_result": snapshot.type_result.model_dump(),
    }
    canonical = _json(
        {
            "schema_version": "ai-explanation-input/v2",
            "scene_contract_version": policy.scene_contract_version,
            "source": snapshot.source.model_dump(),
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
