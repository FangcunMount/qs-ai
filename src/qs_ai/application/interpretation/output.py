"""Deterministic output gates. Semantic safety remains a required separate gate."""

import json
from dataclasses import dataclass
from typing import Any, Protocol

from qs_ai.application.interpretation.preparation import PreparedExplanation


class InvalidOutput(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class OutputParser(Protocol):
    def parse(self, raw: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class DeterministicOutput:
    content_json: str
    validator_version: str = "qs-ai-output-deterministic/v1"


def validate_output(
    raw: str, prepared: PreparedExplanation, parser: OutputParser
) -> DeterministicOutput:
    policy = prepared.release.render_policy
    if len(raw) > policy.max_output_characters:
        raise InvalidOutput("output_too_long")
    content = parser.parse(raw)
    facts = json.loads(prepared.assembled_input.provider_payload)["facts"]
    dimensions = {item["ref"]: item for item in facts["dimensions"]}
    suggestions = {item["ref"] for item in facts["standard_suggestions"]}
    allowed_refs = {
        "dimension": set(dimensions),
        "standard_suggestion": suggestions,
        "overall_result": {"overall_result"},
        "model_result": {"model_result"} if facts["model_result"] is not None else set(),
    }
    insights, advice = content["integrated_insights"], content["suggestions"]
    for item in (*insights, *advice):
        if any(ref["ref"] not in allowed_refs[ref["kind"]] for ref in item["evidence_refs"]):
            raise InvalidOutput("unresolved_evidence")
    for item in advice:
        if any(ref not in suggestions for ref in item["source_suggestion_refs"]):
            raise InvalidOutput("unresolved_standard_suggestion")
    if not policy.insight_min_items <= len(insights) <= policy.insight_max_items:
        raise InvalidOutput("insight_count_outside_policy")
    for item in insights:
        if item["kind"] not in policy.allowed_insight_kinds:
            raise InvalidOutput("insight_kind_not_allowed")
        refs = {ref["ref"] for ref in item["evidence_refs"] if ref["kind"] == "dimension"}
        if not policy.min_dimension_refs <= len(refs) <= policy.max_dimension_refs:
            raise InvalidOutput("dimension_count_outside_policy")
        if not policy.allow_parent_child_in_same_insight:
            for ref in refs:
                # Follow the entire available lineage, not only the immediate parent.
                seen = {ref}
                parent = dimensions[ref]["parent_ref"]
                while parent is not None:
                    if parent in seen:
                        raise InvalidOutput("invalid_input_hierarchy")
                    if parent in refs:
                        raise InvalidOutput("ancestor_descendant_combination")
                    seen.add(parent)
                    parent = dimensions.get(parent, {}).get("parent_ref")
    if not policy.suggestion_min_items <= len(advice) <= policy.suggestion_max_items:
        raise InvalidOutput("suggestion_count_outside_policy")
    for item in advice:
        if (
            item["origin"] not in policy.allowed_suggestion_origins
            or item["category"] not in policy.allowed_suggestion_categories
        ):
            raise InvalidOutput("suggestion_policy_mismatch")
        if len(item["actions"]) > policy.max_actions_per_item:
            raise InvalidOutput("too_many_actions")
    return DeterministicOutput(json.dumps(content, ensure_ascii=False, separators=(",", ":")))
