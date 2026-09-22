"""Strict migration decoder for QS published Profile definitions."""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qs_ai.application.interpretation.input import InputPolicy, MBTIInputPolicy
from qs_ai.application.interpretation.prompts import RenderPolicy
from qs_ai.application.interpretation.release import ExplanationRelease, InvalidRelease
from qs_ai.domain.governance.scenes import MBTI_AXES
from qs_ai.infrastructure.qs_server.prompts import prompt_directory

Version = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")]
Route = Annotated[str, Field(pattern=r"^[a-z][a-z0-9._-]{0,63}$")]
Text = Annotated[str, Field(min_length=1, max_length=255)]


class PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    @model_validator(mode="after")
    def unique_lists(self) -> Self:
        for value in self.__dict__.values():
            if isinstance(value, list):
                if any(not x.strip() or len(x.encode()) > 255 for x in value):
                    raise ValueError("Invalid policy code")
                if len({x.strip() for x in value}) != len(value):
                    raise ValueError("Duplicate policy code")
        return self


class Selector(PolicyModel):
    audience: Literal["participant"]
    model_kind: Literal["scale"]
    decision_kind: Literal["score_range"]
    model_code: Text | None
    model_version: Version | None

    @model_validator(mode="after")
    def version_needs_code(self) -> Self:
        if self.model_version is not None and self.model_code is None:
            raise ValueError("Model version requires code")
        if self.model_code is not None and not self.model_code.strip():
            raise ValueError("Model code is empty")
        return self


class Eligibility(PolicyModel):
    min_eligible_dimensions: int = Field(ge=2, le=50)
    max_input_dimensions: int = Field(ge=2, le=50)
    eligible_dimension_codes: list[Text] = Field(max_length=50)
    excluded_dimension_codes: list[Text] = Field(max_length=50)
    on_dimension_overflow: Literal["reject"]

    @model_validator(mode="after")
    def bounds(self) -> Self:
        if self.min_eligible_dimensions > self.max_input_dimensions:
            raise ValueError("Invalid dimension bounds")
        if {x.strip() for x in self.eligible_dimension_codes} & {
            x.strip() for x in self.excluded_dimension_codes
        }:
            raise ValueError("Overlapping dimension policy")
        return self


class Hierarchy(PolicyModel):
    allow_parent_child_in_same_insight: bool


class Input(PolicyModel):
    context_scope: Literal["current_assessment_only"]
    include_norm_context: bool
    include_model_result: bool
    allowed_focus_areas: list[Route] = Field(max_length=20)
    hierarchy_policy: Hierarchy


class Insight(PolicyModel):
    allowed_kinds: list[
        Literal[
            "reinforcing_pattern",
            "contrasting_pattern",
            "combined_strength",
            "combined_attention",
            "context_dependent_pattern",
        ]
    ] = Field(min_length=1)
    min_items: int = Field(ge=1, le=8)
    max_items: int = Field(ge=1, le=8)
    min_dimension_refs_per_item: int = Field(ge=2, le=6)
    max_dimension_refs_per_item: int = Field(ge=2, le=6)
    allow_causal_claims: bool

    @model_validator(mode="after")
    def bounds(self) -> Self:
        if (
            self.min_items > self.max_items
            or self.min_dimension_refs_per_item > self.max_dimension_refs_per_item
            or self.allow_causal_claims
        ):
            raise ValueError("Invalid insight policy")
        return self


class Suggestion(PolicyModel):
    allowed_origins: list[Literal["standard_derived", "generated_low_risk"]] = Field(min_length=1)
    allowed_categories: list[Route] = Field(min_length=1)
    min_items: int = Field(ge=1, le=8)
    max_items: int = Field(ge=1, le=8)
    max_actions_per_item: int = Field(ge=1, le=5)
    require_evidence_refs: bool
    require_standard_refs_for_standard_derived: bool

    @model_validator(mode="after")
    def bounds(self) -> Self:
        if (
            self.min_items > self.max_items
            or not self.require_evidence_refs
            or not self.require_standard_refs_for_standard_derived
        ):
            raise ValueError("Invalid suggestion policy")
        return self


class Safety(PolicyModel):
    policy_version: Version
    forbidden_claims: list[str]
    disclaimer_version: Version

    @model_validator(mode="after")
    def retain_forbidden_claims(self) -> Self:
        if set(self.forbidden_claims) != {
            "diagnosis",
            "causality",
            "medication",
            "treatment_plan",
            "risk_reclassification",
            "identity_inference",
            "deterministic_future_prediction",
        }:
            raise ValueError("Required safety boundaries changed")
        return self


class GenerationBase(PolicyModel):
    prompt_template_id: Text
    prompt_version: Version
    provider_route: Route
    output_schema_version: Literal["ai-explanation-output/v1"]
    max_output_characters: int = Field(ge=512, le=20000)

    @model_validator(mode="after")
    def nonempty_template(self) -> Self:
        if not self.prompt_template_id.strip():
            raise ValueError("Prompt template is empty")
        return self


class Generation(GenerationBase):
    input_schema_version: Literal["ai-explanation-input/v1"]


class MBTIGeneration(GenerationBase):
    input_schema_version: Literal["ai-explanation-input/v2"]


class MBTISelector(PolicyModel):
    audience: Literal["participant"]
    model_kind: Literal["typology"]
    decision_kind: Literal["pole_composition"]
    model_code: Literal["MBTI_OEJTS"]
    model_version: Literal["v64-report-202608-v1"]


class DefinitionBase(PolicyModel):
    profile_id: Text
    version: Version
    eligibility: Eligibility
    input_policy: Input
    insight_policy: Insight
    suggestion_policy: Suggestion
    safety_policy: Safety

    @model_validator(mode="after")
    def nonempty_id(self) -> Self:
        if not self.profile_id.strip():
            raise ValueError("Profile id is empty")
        return self


class Definition(DefinitionBase):
    schema_version: Literal["ai-explanation-profile/v1"]
    selector: Selector
    generation_policy: Generation


class MBTIDefinition(DefinitionBase):
    schema_version: Literal["ai-explanation-profile/v2"]
    scene_contract_version: Literal["mbti-single-assessment/v1"]
    selector: MBTISelector
    generation_policy: MBTIGeneration

    @model_validator(mode="after")
    def complete_mbti(self) -> Self:
        e, i = self.eligibility, self.input_policy
        if (
            (e.min_eligible_dimensions, e.max_input_dimensions) != (4, 4)
            or e.eligible_dimension_codes != [axis[0] for axis in MBTI_AXES]
            or e.excluded_dimension_codes
            or i.include_norm_context
            or not i.include_model_result
        ):
            raise ValueError("MBTI requires all four axes and model result without norms")
        return self


def decode_profile_definition(value: Any) -> Definition | MBTIDefinition:
    if isinstance(value, dict) and value.get("schema_version") == "ai-explanation-profile/v2":
        return MBTIDefinition.model_validate(value)
    return Definition.model_validate(value)


def canonical_definition(value: dict[str, Any]) -> str:
    raw = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    # Match Go encoding/json's HTML-safe canonical map encoding for fingerprints.
    for character, escaped in (
        ("&", "\\u0026"),
        ("<", "\\u003c"),
        (">", "\\u003e"),
        ("\u2028", "\\u2028"),
        ("\u2029", "\\u2029"),
    ):
        raw = raw.replace(character, escaped)
    return raw


def decode_published_profile(envelope: dict[str, Any]) -> ExplanationRelease:
    try:
        if (
            set(envelope) != {"definition", "fingerprint", "status"}
            or envelope["status"] != "published"
        ):
            raise InvalidRelease("A published Profile envelope is required")
        definition = decode_profile_definition(envelope["definition"])
        canonical = canonical_definition(definition.model_dump())
        fingerprint = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
        if fingerprint != envelope["fingerprint"]:
            raise InvalidRelease("Profile fingerprint mismatch")
        selector, eligibility, inputs = (
            definition.selector,
            definition.eligibility,
            definition.input_policy,
        )
        insight, suggestion, generation = (
            definition.insight_policy,
            definition.suggestion_policy,
            definition.generation_policy,
        )
        input_policy = InputPolicy(
            definition.profile_id,
            definition.version,
            fingerprint,
            selector.model_code,
            selector.model_version,
            eligibility.min_eligible_dimensions,
            eligibility.max_input_dimensions,
            tuple(eligibility.eligible_dimension_codes),
            tuple(eligibility.excluded_dimension_codes),
            tuple(inputs.allowed_focus_areas),
            inputs.include_norm_context,
            inputs.include_model_result,
        )
        if isinstance(definition, MBTIDefinition):
            input_policy = MBTIInputPolicy(
                **vars(input_policy), scene_contract_version=definition.scene_contract_version
            )
        return ExplanationRelease(
            input_policy=input_policy,
            render_policy=RenderPolicy(
                generation.prompt_template_id,
                generation.prompt_version,
                tuple(inputs.allowed_focus_areas),
                tuple(insight.allowed_kinds),
                insight.min_items,
                insight.max_items,
                insight.min_dimension_refs_per_item,
                insight.max_dimension_refs_per_item,
                inputs.hierarchy_policy.allow_parent_child_in_same_insight,
                tuple(suggestion.allowed_origins),
                tuple(suggestion.allowed_categories),
                suggestion.min_items,
                suggestion.max_items,
                suggestion.max_actions_per_item,
                generation.max_output_characters,
            ),
            provider_route=generation.provider_route,
            definition_json=canonical,
        )
    except (ValueError, TypeError, KeyError):
        raise InvalidRelease("Invalid published Profile or fingerprint") from None


def load_migrated_release(
    profile_id: str, version: str, *, directory: Path | None = None
) -> ExplanationRelease:
    directory = directory if directory is not None else prompt_directory()
    try:
        baseline = json.loads((directory / "published-profile-baseline.json").read_bytes())
        matches = [
            entry
            for entry in baseline["profiles"]
            if entry["definition"]["profile_id"] == profile_id
            and entry["definition"]["version"] == version
        ]
        if len(matches) != 1:
            raise InvalidRelease("Missing or ambiguous Profile version")
        return decode_published_profile(matches[0])
    except (OSError, ValueError, KeyError, TypeError):
        raise InvalidRelease("Cannot load published migration Profile") from None
