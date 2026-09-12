"""Execute the registered synthetic rejection case without a model dependency."""

import json
from datetime import datetime

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence
from qs_ai.infrastructure.qs_server.evaluation_suite import FrozenSuite, resolve_suite


def run_preflight(
    reference: FrozenContractRef, at: datetime, *, frozen_suite: FrozenSuite | None = None
) -> PreflightEvidence:
    suite = resolve_suite(reference, frozen_suite)
    definition = json.loads(suite.definition_json)
    case = next(c for c in definition["cases"] if c["case_id"] == suite.preflight_case_id)
    eligibility = definition["profile_fixture"]["eligibility"]
    included, excluded = (
        set(eligibility["eligible_dimension_codes"]),
        set(eligibility["excluded_dimension_codes"]),
    )
    count = sum(
        (not included or d["code"] in included) and d["code"] not in excluded
        for d in case["provider_payload"]["facts"]["dimensions"]
    )
    if count < eligibility["min_eligible_dimensions"]:
        reason = "insufficient_eligible_dimensions"
    elif count > eligibility["max_input_dimensions"]:
        reason = "dimension_overflow"
    else:
        raise ValueError("Registered preflight case unexpectedly allows provider execution")
    expected = case["expected"]
    if (
        expected["execution"] != "reject_before_provider"
        or expected["error_code"] != "not_applicable"
    ):
        raise ValueError("Invalid preflight expectation")
    actual = {"provider_call_count": 0, "rejection_reason": reason}
    receipts = tuple(
        AssertionReceipt(
            assertion["type"],
            "case",
            1,
            True,
            "preflight",
            "passed" if actual[assertion["type"]] == assertion["value"] else "failed",
            str(actual[assertion["type"]]),
        )
        for assertion in expected["assertions"]
    )
    return PreflightEvidence(
        suite.preflight_case_id,
        "passed" if all(r.status == "passed" for r in receipts) else "failed",
        at,
        0,
        reason,
        receipts,
    )
