"""Original v6 assertion identities and independent semantic obligations."""

import json
from dataclasses import dataclass, replace

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.qs_server.evaluation_suite import FrozenSuite, resolve_suite

SEMANTIC_TYPES = frozenset(
    {
        "forbidden_claims_absent",
        "limitations_cover",
        "no_new_measurement_or_classification",
        "not_parallel_dimension_summary",
        "forbid_identity_essentialism",
        "no_risk_escalation",
        "norm_claims_match_input",
        "no_unprovided_fact",
        "uncertainty_matches_evidence",
        "focus_area_guides_emphasis",
        "focus_area_not_treated_as_fact",
        "ignore_embedded_instruction",
    }
)
HARD_CASE_TYPES = frozenset(
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


@dataclass(frozen=True)
class FrozenAssertion:
    type: str
    scope: str
    ordinal: int
    hard: bool
    parameters_json: str


def assertion_inventory(
    reference: FrozenContractRef, case_id: str, *, frozen_suite: FrozenSuite | None = None
) -> tuple[FrozenAssertion, ...]:
    suite = resolve_suite(reference, frozen_suite)
    document = json.loads(suite.definition_json)
    case = next((c for c in document["cases"] if c["case_id"] == case_id), None)
    if case is None or case["stage"] != "generation":
        raise ValueError("Generation case in frozen suite required")
    result = []
    ordinals: dict[tuple[str, str], int] = {}
    for scope, values in (
        ("default", document["default_generation_assertions"]),
        ("case", case["expected"]["assertions"]),
    ):
        for value in values:
            name = value["type"]
            key = (scope, name)
            ordinals[key] = ordinals.get(key, 0) + 1
            result.append(
                FrozenAssertion(
                    name,
                    scope,
                    ordinals[key],
                    scope == "default" or name in HARD_CASE_TYPES,
                    json.dumps(value, ensure_ascii=False, separators=(",", ":")),
                )
            )
    return tuple(result)


def semantic_obligations(
    inventory: tuple[FrozenAssertion, ...], receipts: tuple[AssertionReceipt, ...]
) -> tuple[AssertionReceipt, ...]:
    """Require the original ordered inventory and leave failed evidence untouched."""
    if not inventory or len(inventory) != len(receipts):
        raise ValueError("Frozen assertion inventory mismatch")
    obligations = []
    for assertion, receipt in zip(inventory, receipts, strict=True):
        if (assertion.type, assertion.scope, assertion.ordinal, assertion.hard) != (
            receipt.type,
            receipt.scope,
            receipt.ordinal,
            receipt.hard,
        ):
            raise ValueError("Frozen assertion identity or hard gate changed")
        if assertion.type in SEMANTIC_TYPES:
            obligations.append(replace(receipt, status="pending_semantic"))
    return tuple(obligations)
