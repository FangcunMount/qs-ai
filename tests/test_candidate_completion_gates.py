import json
from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.domain.evaluation.acceptance import RULE_VERSION, acceptance_version, rule_document
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.quality_gates import evaluate_quality_gates
from tests.test_quality_gates import AT, complete, fail_cases


def recovered():
    values = complete()
    first = values["semantics"][0]
    failed = replace(
        first,
        execution_id="semantic:original-failure",
        status="failed",
        started_at=AT - timedelta(seconds=3),
        finished_at=AT - timedelta(seconds=2),
        failure=ClassifiedFailure(
            "semantic_evaluation",
            "semantic_execution",
            "semantic_decision_contract_invalid",
            True,
            False,
            "retry_semantic",
            "缺少检查项",
            ("semantic:original-failure",),
        ),
    )
    final = replace(first, execution_ordinal=2)
    values["semantics"] = (failed, final, *values["semantics"][1:])
    item = values["candidates"][0]
    values["candidates"] = (
        replace(item, evidence=replace(item.evidence, semantic=final)),
        *values["candidates"][1:],
    )
    return values


def test_final_candidate_success_is_gate_while_failed_attempt_stays_observable():
    values = recovered()
    original = values["semantics"]
    legacy = evaluate_quality_gates(**values)
    result = evaluate_quality_gates(**values, acceptance_rule=RULE_VERSION)
    assert dict(legacy.gate_passes)["G3"] is False
    assert dict(result.gate_passes) == {"G3": True, "G4": True, "G5": True}
    metrics = {m.name: m for m in result.metrics}
    assert metrics["candidate_completion_rate"].value == 1.0
    assert metrics["candidate_completion_rate"].threshold == 1.0
    assert metrics["observed_semantic_execution_success_rate"].value == 35 / 36
    assert metrics["observed_semantic_execution_success_rate"].threshold is None
    assert metrics["observed_semantic_first_attempt_success_rate"].value == 34 / 35
    assert metrics["observed_semantic_retry_count"].value == 1
    assert values["semantics"] == original and original[0].status == "failed"
    assert evaluate_quality_gates(**values) == legacy


def test_quality_and_human_rejection_still_block_release():
    values = fail_cases(recovered(), [0], hard=True)
    values["reviews"] = (replace(values["reviews"][0], decision="reject"), *values["reviews"][1:])
    result = evaluate_quality_gates(**values, acceptance_rule=RULE_VERSION)
    assert dict(result.gate_passes) == {"G3": True, "G4": False, "G5": False}


@pytest.mark.parametrize(
    "case", ["missing_candidate", "missing_semantic", "changed_candidate", "unknown_rule"]
)
def test_new_rule_requires_complete_matching_successful_evidence(case):
    values = recovered()
    if case == "missing_candidate":
        values["candidates"] = values["candidates"][:-1]
    if case == "missing_semantic":
        values["semantics"] = values["semantics"][:-1]
    if case == "changed_candidate":
        target = values["candidates"][0]
        values["candidates"] = (
            replace(target, generation_execution_id="missing"),
            *values["candidates"][1:],
        )
    with pytest.raises(ValueError):
        evaluate_quality_gates(
            **values, acceptance_rule="unknown" if case == "unknown_rule" else RULE_VERSION
        )


def adoption():
    return {
        "rule": rule_document(),
        "actor": "operator:root",
        "reason": "采用最终完成口径",
        "adopted_at": AT.isoformat(),
        "source_version": 217,
        "version": 218,
        "release_fingerprint": "sha256:" + "a" * 64,
    }


def test_legacy_and_new_runs_have_distinct_explicit_acceptance_versions():
    creation = {"release_fingerprint": "sha256:" + "a" * 64}
    progress = {
        "transitions": [{"cause_code": "candidate_evidence_complete", "at": AT.isoformat()}]
    }
    assert acceptance_version(creation, progress, 217, AT) is None
    assert (
        acceptance_version({**creation, "acceptance_rule": rule_document()}, progress, 217, AT)
        == RULE_VERSION
    )
    progress["acceptance_rule_adoption"] = adoption()
    assert acceptance_version(creation, progress, 218, AT) == RULE_VERSION


@pytest.mark.parametrize("case", ["fingerprint", "version", "time", "rule", "actor", "reason"])
def test_adoption_audit_cannot_drift(case):
    value = adoption()
    if case == "fingerprint":
        value["release_fingerprint"] = "changed"
    if case == "version":
        value["version"] = 217
    if case == "time":
        value["adopted_at"] = (AT + timedelta(seconds=1)).isoformat()
    if case == "rule":
        value["rule"] = json.loads(
            json.dumps(rule_document()).replace("observation_only", "always_pass")
        )
    if case == "actor":
        value["actor"] = ""
    if case == "reason":
        value["reason"] = ""
    with pytest.raises(ValueError):
        acceptance_version(
            {"release_fingerprint": "sha256:" + "a" * 64},
            {
                "acceptance_rule_adoption": value,
                "transitions": [
                    {"cause_code": "candidate_evidence_complete", "at": AT.isoformat()}
                ],
            },
            218,
            AT,
        )
