import json
from dataclasses import replace

import pytest

from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.qs_server.evaluation_assertions import (
    assertion_inventory,
    semantic_obligations,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import V6


def receipts(inventory):
    return tuple(
        AssertionReceipt(
            a.type, a.scope, a.ordinal, a.hard, "deterministic", "failed", "original failure"
        )
        for a in inventory
    )


def test_original_scope_ordinals_hard_gates_and_parameters_are_preserved():
    inventory = assertion_inventory(V6, "PROMPT-EVAL-007")
    assert len(inventory) == 12
    assert all(a.scope == "default" and a.hard for a in inventory[:8])
    repeated = [a for a in inventory if a.type == "forbid_dimension_group"]
    assert [a.ordinal for a in repeated] == [1, 2]
    assert all(a.scope == "case" and a.hard for a in repeated)
    assert json.loads(repeated[1].parameters_json)["dimension_refs"] == [
        "dimension:regulation_total",
        "dimension:attention_shift",
    ]
    assert inventory[-1].hard is False


def test_independent_semantics_survive_deterministic_failure_without_mutating_it():
    inventory = assertion_inventory(V6, "PROMPT-EVAL-004")
    frozen = receipts(inventory)
    pending = semantic_obligations(inventory, frozen)
    assert {a.type for a in pending} == {
        "no_new_measurement_or_classification",
        "forbidden_claims_absent",
        "limitations_cover",
        "no_unprovided_fact",
        "uncertainty_matches_evidence",
    }
    assert all(a.status == "pending_semantic" for a in pending)
    assert all(a.status == "failed" and a.detail == "original failure" for a in frozen)


@pytest.mark.parametrize("damage", ["missing", "extra", "order", "scope", "ordinal", "hard"])
def test_frozen_inventory_drift_is_rejected(damage):
    inventory = assertion_inventory(V6, "PROMPT-EVAL-001")
    frozen = list(receipts(inventory))
    if damage == "missing":
        frozen.pop()
    elif damage == "extra":
        frozen.append(frozen[0])
    elif damage == "order":
        frozen[0], frozen[1] = frozen[1], frozen[0]
    else:
        frozen[0] = replace(
            frozen[0], **{damage: {"scope": "case", "ordinal": 2, "hard": False}[damage]}
        )
    with pytest.raises(ValueError, match="Frozen assertion"):
        semantic_obligations(inventory, tuple(frozen))


@pytest.mark.parametrize("case", ["PROMPT-EVAL-008", "missing"])
def test_preflight_and_unknown_case_are_not_generation_inventory(case):
    with pytest.raises(ValueError):
        assertion_inventory(V6, case)
