import json
from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.domain.evaluation.completion import GenerationCompletion, ProviderReceipt
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.quality_gates import (
    SCORE_NAMES,
    QualityCandidate,
    assertion_outcome,
    evaluate_quality_gates,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_quality_thresholds
from tests.test_evaluation_review import AT, CLOSED, candidate, contradiction, review


def complete():
    candidates, generations, semantics, reviews = [], [], [], []
    for i in range(35):
        target = candidate(f"candidate:{i}")
        default = replace(target.assertions[0], status="passed")
        assertion = AssertionReceipt(
            "case_rule", "case", 1, False, "deterministic/v1", "passed", "案例通过"
        )
        payload = json.loads(target.semantic.normalized_output)
        payload["decisions"][0]["status"] = "passed"
        payload["scores"] = dict.fromkeys(SCORE_NAMES, 5)
        raw = json.dumps(payload).encode()
        semantic = replace(
            target.semantic,
            execution_id=f"semantic:{i}",
            invocation_id=f"judge-call:{i}",
            receipt=ProviderReceipt(
                f"judge-call:{i}", f"judge-response:{i}", "deepseek", "model", 1, 2, 100
            ),
            raw_output=raw,
            normalized_output=raw,
        )
        target = replace(
            target,
            semantic=semantic,
            assertions=(default, assertion),
            semantic_assertions=(default,),
        )
        case_id, ordinal = f"case:{i // 5}", i % 5 + 1
        generated = GenerationCompletion(
            f"generation:{i}",
            case_id,
            ordinal,
            1,
            f"call:{i}",
            "succeeded",
            AT - timedelta(seconds=1),
            AT,
            1,
            ProviderReceipt(f"call:{i}", f"response:{i}", "deepseek", "model", 1, 2, 100),
            target.normalized_output,
            target.normalized_output,
            semantic.candidate_output_fingerprint,
        )
        candidates.append(QualityCandidate(case_id, ordinal, generated.execution_id, target))
        generations.append(generated)
        semantics.append(semantic)
        reviews.extend(
            (
                review(candidate_id=target.candidate_id),
                review(candidate_id=target.candidate_id, role="safety_product", reviewer="user:43"),
            )
        )
    return dict(
        candidates=tuple(candidates),
        generations=tuple(generations),
        semantics=tuple(semantics),
        reviews=tuple(reviews),
        thresholds=load_quality_thresholds(),
        closed_at=CLOSED,
        evaluated_at=CLOSED + timedelta(seconds=1),
    )


def calculate(values):
    return evaluate_quality_gates(**values)


def fail_cases(values, indices, *, hard=False):
    changed = list(values["candidates"])
    for i in indices:
        item = changed[i]
        assertions = list(item.evidence.assertions)
        index = 0 if hard else 1
        assertions[index] = replace(assertions[index], status="failed")
        changed[i] = replace(item, evidence=replace(item.evidence, assertions=tuple(assertions)))
    return {**values, "candidates": tuple(changed)}


def test_frozen_thresholds_and_all_passed_sample():
    values = complete()
    t = values["thresholds"]
    assert (t.generation_cases, t.candidates_per_case, t.review_count) == (7, 5, 70)
    assert (t.min_infrastructure_rate, t.min_contract_rate, t.min_semantic_rate) == (
        0.98,
        0.95,
        0.98,
    )
    assert (t.min_case_passes, t.min_overall_passes) == (4, 32)
    result = calculate(values)
    assert dict(result.gate_passes) == {"G3": True, "G4": True, "G5": True}
    assert result.reasons == () and result.semantic_adjudications == ()
    assert [(m.numerator, m.denominator) for m in result.metrics] == [(70, 70), (35, 35), (35, 35)]


@pytest.mark.parametrize(
    "indices,code",
    [
        ([0, 5, 10], None),
        ([0, 5, 10, 15], "case_assertion_overall_failed"),
        ([0, 1], "case_assertion_stability_failed"),
    ],
)
def test_case_stability_and_overall_boundaries(indices, code):
    result = calculate(fail_cases(complete(), indices))
    if code is None:
        assert dict(result.gate_passes)["G4"]
    else:
        assert code in {r.code for r in result.reasons}


def test_one_default_failure_blocks_quality_even_with_35_case_passes():
    result = calculate(fail_cases(complete(), [0], hard=True))
    assert "candidate_hard_assertion_failed" in {r.code for r in result.reasons}
    assert dict(result.gate_passes)["G3"] and dict(result.gate_passes)["G5"]


def test_assertions_group_by_scope_type_ordinal_and_explicit_failure_wins():
    value = AssertionReceipt("rule", "case", 1, False, "first", "pending_semantic", "")
    passed = replace(value, status="passed", evaluator="judge")
    assert assertion_outcome((value, passed)) == (True, True, True)
    failed = replace(value, status="failed")
    assert assertion_outcome((failed, passed)) == (True, False, True)
    assert assertion_outcome((replace(passed, scope="default"),)) == (False, True, True)
    assert assertion_outcome((replace(failed, scope="default"),)) == (False, True, False)


def test_missing_reviews_and_any_rejection_block_accountability_only():
    values = complete()
    result = calculate({**values, "reviews": values["reviews"][:-1]})
    assert {r.code for r in result.reasons} == {
        "human_review_incomplete",
        "human_review_count_incomplete",
    }
    reviews = (replace(values["reviews"][0], decision="reject"), *values["reviews"][1:])
    result = calculate({**values, "reviews": reviews})
    assert {r.code for r in result.reasons} == {"human_review_rejected"}


@pytest.mark.parametrize(
    "score,code",
    [(3, "candidate_semantic_score_below_minimum"), (4, "semantic_average_below_threshold")],
)
def test_semantic_minimum_and_average_are_independent(score, code):
    values = complete()
    candidates, semantics = list(values["candidates"]), list(values["semantics"])
    indices = range(35) if score == 4 else [0]
    for i in indices:
        raw = json.loads(semantics[i].normalized_output)
        raw["scores"]["faithfulness"] = score
        output = json.dumps(raw).encode()
        semantics[i] = replace(semantics[i], raw_output=output, normalized_output=output)
        candidates[i] = replace(
            candidates[i], evidence=replace(candidates[i].evidence, semantic=semantics[i])
        )
    result = calculate({**values, "candidates": tuple(candidates), "semantics": tuple(semantics)})
    assert code in {r.code for r in result.reasons}


@pytest.mark.parametrize("kind", ["contract", "infrastructure", "semantic"])
def test_failed_calls_keep_distinct_reliability_denominators(kind):
    values = complete()
    if kind == "semantic":
        failure = ClassifiedFailure(
            "semantic_evaluation",
            "semantic_execution",
            "semantic_output_schema_invalid",
            False,
            False,
            "retry_semantic",
            "结构错误",
            ("evidence:1",),
        )
        original = values["semantics"][0]
        failed = replace(
            original,
            execution_id="semantic:failed",
            invocation_id="judge:failed",
            status="failed",
            failure=failure,
            normalized_output=b"",
            receipt=replace(original.receipt, invocation_id="judge:failed"),
        )
        values["semantics"] += (failed,)
    else:
        originals = values["generations"]
        for i in range(2):
            failure = ClassifiedFailure(
                "output_validation" if kind == "contract" else "generation_execution",
                "output_contract_conformance" if kind == "contract" else "infrastructure_execution",
                "output_schema_invalid" if kind == "contract" else "timeout",
                False,
                False,
                "replace_generation" if kind == "contract" else "retry_generation",
                "调用失败",
                ("evidence:1",),
            )
            g = originals[i]
            failed = replace(
                g,
                execution_id=f"failed:{i}",
                invocation_id=f"failed-call:{i}",
                status="failed",
                failure=failure,
                normalized_output=b"",
                normalized_fingerprint="",
                raw_output=g.raw_output if kind == "contract" else b"",
                receipt=replace(g.receipt, invocation_id=f"failed-call:{i}")
                if kind == "contract"
                else None,
            )
            values["generations"] += (failed,)
    result = calculate(values)
    expected = {
        "contract": "generation_contract_conformance_rate_below_threshold",
        "infrastructure": "infrastructure_success_rate_below_threshold",
        "semantic": "semantic_execution_success_rate_below_threshold",
    }[kind]
    assert {r.code for r in result.reasons} == {expected}


@pytest.mark.parametrize("case", ["missing", "duplicate", "missing_execution", "early"])
def test_incomplete_or_misbound_inventory_is_not_a_quality_report(case):
    values = complete()
    if case == "missing":
        values["candidates"] = values["candidates"][:-1]
    if case == "duplicate":
        values["candidates"] = (values["candidates"][1], *values["candidates"][1:])
    if case == "missing_execution":
        values["generations"] = values["generations"][1:]
    if case == "early":
        values["evaluated_at"] = AT
    with pytest.raises(ValueError):
        calculate(values)


@pytest.mark.parametrize("kind", ["generation", "semantic"])
def test_resolved_unknown_calls_remain_in_reliability_denominator(kind):
    # G1/G2 must validate manual resolution separately; retaining unknown
    # terminal evidence must not improve reliability after a replacement.
    values = complete()
    key = "generations" if kind == "generation" else "semantics"
    for i in range(2):
        original = values[key][i]
        changes = dict(
            execution_id=f"unknown:{kind}:{i}",
            invocation_id=f"unknown-call:{kind}:{i}",
            status="result_unknown",
            receipt=None,
            normalized_output=b"",
            failure=ClassifiedFailure(
                "generation_execution" if kind == "generation" else "semantic_evaluation",
                "result_unknown",
                "response_lost",
                False,
                True,
                "manual_acknowledgement",
                "响应未知",
                ("evidence:unknown",),
            ),
        )
        if kind == "generation":
            changes["normalized_fingerprint"] = ""
        values[key] += (replace(original, **changes),)
    result = calculate(values)
    assert [(m.numerator, m.denominator) for m in result.metrics] == [
        (70, 72),
        (35, 35),
        (35, 37 if kind == "semantic" else 35),
    ]
    assert not dict(result.gate_passes)["G3"]


@pytest.mark.parametrize("sign_count", [0, 1, 2])
def test_quality_gate_uses_only_complete_dual_semantic_adjudication(sign_count):
    values = complete()
    item = values["candidates"][0]
    failed = replace(item.evidence.assertions[0], status="failed")
    payload = json.loads(item.evidence.semantic.normalized_output)
    payload["decisions"][0]["status"] = "failed"
    output = json.dumps(payload).encode()
    semantic = replace(item.evidence.semantic, raw_output=output, normalized_output=output)
    target = replace(
        item.evidence,
        semantic=semantic,
        assertions=(failed, item.evidence.assertions[1]),
        semantic_assertions=(failed,),
    )
    values["candidates"] = (replace(item, evidence=target), *values["candidates"][1:])
    values["semantics"] = (semantic, *values["semantics"][1:])
    reviews = list(values["reviews"])
    for i in range(sign_count):
        reviews[i] = replace(
            reviews[i],
            semantic_review=contradiction(
                execution_id=semantic.execution_id, output_fingerprint=semantic.output_fingerprint
            ),
        )
    values["reviews"] = tuple(reviews)
    result = calculate(values)
    assert dict(result.gate_passes)["G4"] == (sign_count == 2)
    assert len(result.semantic_adjudications) == (1 if sign_count == 2 else 0)
    assert target.assertions[0].status == "failed"
    assert values["semantics"][0] == semantic


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation_cases", None),
        ("candidates_per_case", True),
        ("min_infrastructure_rate", float("nan")),
        ("review_count", 69),
        ("minimum_scores", (4, 3)),
        ("minimum_averages", (6,) * 5),
    ],
)
def test_invalid_thresholds_reject_before_calculation(field, value):
    with pytest.raises(ValueError):
        replace(load_quality_thresholds(), **{field: value})
