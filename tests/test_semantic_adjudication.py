import json
from dataclasses import replace

import pytest

from qs_ai.domain.evaluation.adjudication import effective_candidate_assertions
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from tests.test_evaluation_review import CLOSED, candidate, contradiction, review


def effective(values=(), target=None, **kwargs):
    return effective_candidate_assertions(
        target or candidate(), values, CLOSED, gate_policy_version=kwargs.get("policy", "v2")
    )


def signatures():
    first = review(semantic_review=contradiction())
    second = replace(first, reviewer="user:43", role="safety_product")
    return first, second


def test_dual_signatures_derive_audited_pass_without_changing_original_evidence():
    target = candidate()
    first, second = signatures()
    for values in ((), (first,)):
        result = effective(values, target)
        assert result.assertions == target.assertions and result.adjudication is None
    result = effective((second, first), target)
    assert result.assertions[0].status == "passed"
    assert result.adjudication.reviewers == ("user:42", "user:43")
    assert result.adjudication.execution_id == target.semantic.execution_id
    assert result.adjudication.output_fingerprint == target.semantic.output_fingerprint
    assert target == candidate() and target.assertions[0].status == "failed"


def test_ordinary_approval_or_rejection_is_not_a_semantic_override():
    first, second = signatures()
    for value in (
        replace(second, semantic_review=None),
        replace(second, semantic_review=None, decision="reject"),
    ):
        result = effective((first, value))
        assert result.adjudication is None and result.assertions[0].status == "failed"


def test_distinct_valid_excerpts_and_reasons_can_confirm_the_same_assertion():
    first, second = signatures()
    second = replace(
        second,
        semantic_review=replace(
            second.semantic_review, candidate_excerpt="仅供参考", reason="独立核对依据"
        ),
    )
    assert effective((first, second)).adjudication is not None


@pytest.mark.parametrize(
    "case",
    [
        "same_person",
        "same_role",
        "foreign_candidate",
        "wrong_output",
        "wrong_execution",
        "wrong_assertion",
        "invented_excerpt",
        "gate_v1",
    ],
)
def test_unbound_or_invalid_history_cannot_derive_a_pass(case):
    first, second = signatures()
    options = {}
    if case == "same_person":
        second = replace(second, reviewer=first.reviewer)
    elif case == "same_role":
        second = replace(second, role=first.role)
    elif case == "foreign_candidate":
        second = replace(second, candidate_id="candidate:other")
    elif case == "gate_v1":
        options["policy"] = "v1"
    else:
        field, value = {
            "wrong_output": ("output_fingerprint", "sha256:" + "b" * 64),
            "wrong_execution": ("execution_id", "other"),
            "wrong_assertion": ("assertion_ordinal", 2),
            "invented_excerpt": ("candidate_excerpt", "不存在"),
        }[case]
        second = replace(second, semantic_review=replace(second.semantic_review, **{field: value}))
    with pytest.raises(ValueError):
        effective((first, second), **options)


def test_semantic_override_preserves_deterministic_and_other_failed_assertions():
    target = candidate()
    failures = (
        AssertionReceipt(
            "fact_binding", "default", 2, True, "deterministic/v1", "failed", "事实不符"
        ),
        AssertionReceipt("case_constraint", "case", 1, False, "judge/v2", "failed", "未通过案例"),
    )
    target = replace(target, assertions=target.assertions + failures)
    result = effective(signatures(), target)
    assert result.assertions[0].status == "passed" and result.assertions[1:] == failures
    assert target.assertions[0].status == "failed"


def test_two_valid_signatures_for_different_assertions_do_not_override_either():
    target = candidate()
    second_assertion = replace(target.assertions[0], ordinal=2, detail="另一条判断")
    payload = json.loads(target.semantic.normalized_output)
    payload["decisions"].append({**payload["decisions"][0], "ordinal": 2, "detail": "另一条判断"})
    raw = json.dumps(payload, ensure_ascii=False).encode()
    semantic = replace(target.semantic, normalized_output=raw, raw_output=raw)
    assertions = target.assertions + (second_assertion,)
    target = replace(
        target, semantic=semantic, assertions=assertions, semantic_assertions=assertions
    )
    first, second = signatures()
    first = replace(
        first,
        semantic_review=replace(
            first.semantic_review, output_fingerprint=semantic.output_fingerprint
        ),
    )
    second = replace(
        second,
        semantic_review=replace(
            second.semantic_review,
            output_fingerprint=semantic.output_fingerprint,
            assertion_ordinal=2,
            original_detail="另一条判断",
        ),
    )
    result = effective((first, second), target)
    assert result.adjudication is None and result.assertions == assertions
