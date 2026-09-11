"""Execute frozen candidate assertions; semantic obligations remain explicitly pending."""

import json
import unicodedata

from qs_ai.application.interpretation.output import (
    DeterministicOutput,
    InvalidOutput,
    validate_output,
)
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.safety import check_safety
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.qs_server.evaluation_assertions import SEMANTIC_TYPES, assertion_inventory
from qs_ai.infrastructure.qs_server.output import QSOutputParser


def _canonical(content: dict) -> str:
    raw = json.dumps(content, ensure_ascii=False, separators=(",", ":"))
    for literal, escaped in (
        ("&", "\\u0026"),
        ("<", "\\u003c"),
        (">", "\\u003e"),
        ("\u2028", "\\u2028"),
        ("\u2029", "\\u2029"),
    ):
        raw = raw.replace(literal, escaped)
    return raw


def evaluate_candidate_assertions(
    raw: bytes, prepared: PreparedExplanation, suite: FrozenContractRef, case_id: str
) -> tuple[AssertionReceipt, ...]:
    inventory = assertion_inventory(suite, case_id)
    parser = QSOutputParser()
    content = None
    validation = ""
    safety = "unavailable"
    canonical = ""
    try:
        content = parser.parse(raw.decode("utf-8"))
        canonical = _canonical(content)
    except (UnicodeError, InvalidOutput):
        validation = "schema"
    if content is not None:
        try:
            validate_output(canonical, prepared, parser)
        except InvalidOutput as error:
            validation = (
                "reference"
                if error.code in {"unresolved_evidence", "unresolved_standard_suggestion"}
                else "profile"
            )
        try:
            check_safety(DeterministicOutput(canonical))
            safety = ""
        except InvalidOutput as error:
            safety = error.code
    facts = json.loads(prepared.assembled_input.provider_payload)["facts"]
    results = []
    for assertion in inventory:
        name = assertion.type
        parameters = json.loads(assertion.parameters_json)
        status, detail = "passed", ""
        if name == "output_schema_valid":
            status = "failed" if validation == "schema" else "passed"
        elif name == "all_references_resolve":
            status = (
                "blocked"
                if validation == "schema"
                else "failed"
                if validation == "reference"
                else "passed"
            )
        elif name == "profile_output_policy_satisfied":
            status = (
                "blocked"
                if validation in ("schema", "reference")
                else "failed"
                if validation
                else "passed"
            )
        elif name == "limitations_cover":
            status = (
                "failed"
                if safety == "limitations_incomplete"
                else "blocked"
                if safety
                else "pending_semantic"
            )
        elif content is None:
            status = "blocked"
        elif name == "forbidden_claims_absent":
            status = "failed" if safety else "pending_semantic"
        elif name in SEMANTIC_TYPES:
            status = "blocked" if safety else "pending_semantic"
        else:
            insights, suggestions = content["integrated_insights"], content["suggestions"]
            groups = [
                {r["ref"] for r in i["evidence_refs"] if r["kind"] == "dimension"} for i in insights
            ]
            origins = {s["origin"] for s in suggestions}
            matched = False
            if name == "each_insight_has_distinct_dimension_refs":
                matched = all(
                    parameters["minimum"] <= len(g) <= parameters["maximum"] for g in groups
                )
            elif name == "output_character_limit":
                matched = 0 < parameters["maximum"] and len(canonical) <= parameters["maximum"]
            elif name == "insight_kind_any_of":
                matched = any(i["kind"] in parameters["values"] for i in insights)
            elif name in ("insight_references_group", "forbid_dimension_group"):
                target = set(parameters["dimension_refs"])
                present = bool(target) and any(target <= g for g in groups)
                matched = present if name == "insight_references_group" else not present
            elif name == "suggestion_origin_present":
                matched = parameters["value"] in origins
            elif name == "suggestion_origins_exact":
                matched = origins == set(parameters["values"])
            elif name == "no_standard_derived_without_sources":
                matched = bool(facts["standard_suggestions"]) or "standard_derived" not in origins
            elif name == "forbid_source_suggestion_ref":
                matched = all(
                    parameters["ref"] not in s["source_suggestion_refs"] for s in suggestions
                )
            elif name == "forbid_literal_substrings":
                normalized = unicodedata.normalize("NFC", canonical).casefold()
                matched = all(
                    unicodedata.normalize("NFC", value).casefold() not in normalized
                    for value in parameters["values"]
                )
            else:
                raise ValueError("Unsupported frozen generation assertion")
            status = "passed" if matched else "failed"
        if status in ("failed", "blocked"):
            detail = (
                "deterministic assertion failed"
                if status == "failed"
                else "earlier validation did not pass"
            )
        elif status == "pending_semantic":
            detail = "independent semantic and human review required"
        results.append(
            AssertionReceipt(
                name,
                assertion.scope,
                assertion.ordinal,
                assertion.hard,
                "deterministic" if status != "pending_semantic" else "semantic-required",
                status,
                detail,
            )
        )
    return tuple(results)
