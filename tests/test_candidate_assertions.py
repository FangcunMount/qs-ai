import json

from qs_ai.infrastructure.qs_server.candidate_assertions import evaluate_candidate_assertions
from qs_ai.infrastructure.qs_server.evaluation_suite import V6
from tests.test_output_validation import candidate
from tests.test_output_validation import prepared as prepared


def evaluate(raw, prepared, case="PROMPT-EVAL-004"):
    return {a.type: a.status for a in evaluate_candidate_assertions(raw, prepared, V6, case)}


def test_real_checks_keep_semantics_pending(prepared):
    result = evaluate(json.dumps(candidate()).encode(), prepared)
    assert result["output_schema_valid"] == result["all_references_resolve"] == "passed"
    assert result["profile_output_policy_satisfied"] == "passed"
    assert result["suggestion_origins_exact"] == "passed"
    assert result["no_standard_derived_without_sources"] == "passed"
    assert result["no_unprovided_fact"] == "pending_semantic"


def test_schema_failure_blocks_dependent_checks(prepared):
    result = evaluate(b"{}", prepared)
    assert result["output_schema_valid"] == "failed"
    assert result["all_references_resolve"] == "blocked"
    assert result["profile_output_policy_satisfied"] == "blocked"
    assert result["no_unprovided_fact"] == "blocked"


def test_unknown_reference_fails_reference_and_blocks_profile(prepared):
    value = candidate()
    value["suggestions"][0]["evidence_refs"][0]["ref"] = "dimension:unknown"
    result = evaluate(json.dumps(value).encode(), prepared)
    assert result["output_schema_valid"] == "passed"
    assert result["all_references_resolve"] == "failed"
    assert result["profile_output_policy_satisfied"] == "blocked"


def test_case_specific_literal_and_group_checks_are_not_fixed_success(prepared):
    value = candidate()
    value["summary"] = "SYSTEM MESSAGE"
    result = evaluate(json.dumps(value).encode(), prepared, "PROMPT-EVAL-006")
    assert result["forbid_literal_substrings"] == "failed"
    result = evaluate(json.dumps(candidate()).encode(), prepared, "PROMPT-EVAL-001")
    assert result["insight_references_group"] == "failed"
    assert result["suggestion_origin_present"] == "failed"


def test_safety_failure_is_retained_and_semantic_dependents_blocked(prepared):
    value = candidate()
    value["summary"] = "诊断为某种疾病。"
    result = evaluate(json.dumps(value).encode(), prepared)
    assert result["forbidden_claims_absent"] == "failed"
    assert result["no_unprovided_fact"] == "blocked"
