import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.domain.evaluation.completion import ProviderReceipt
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.domain.evaluation.review import (
    CONTRADICTION_POLICY,
    CandidateHumanReview,
    ReviewCandidate,
    SemanticContradictionReview,
    add_human_reviews,
)
from qs_ai.domain.evaluation.semantic_completion import SemanticCompletion

AT = datetime(2026, 9, 12, tzinfo=UTC)
CLOSED = AT + timedelta(seconds=3)


def candidate(identity="candidate:1"):
    output = '{"interpretation":"量表结果仅供参考"}'.encode()
    assertion = AssertionReceipt(
        "forbidden_claims_absent", "default", 1, True, "judge/v2", "failed", "原始判定"
    )
    normalized = json.dumps(
        {
            "decisions": [
                {
                    "type": assertion.type,
                    "scope": assertion.scope,
                    "ordinal": 1,
                    "status": "failed",
                    "detail": assertion.detail,
                }
            ]
        },
        ensure_ascii=False,
    ).encode()
    semantic = SemanticCompletion(
        "semantic:1",
        identity,
        "sha256:" + hashlib.sha256(output).hexdigest(),
        1,
        "invocation:1",
        "succeeded",
        AT + timedelta(seconds=1),
        AT + timedelta(seconds=2),
        1,
        ProviderReceipt("invocation:1", "request:1", "deepseek", "model", 1, 2, 100),
        normalized,
        normalized,
    )
    return ReviewCandidate(identity, AT, output, semantic, "judge/v2", (assertion,), (assertion,))


def review(**changes):
    return replace(
        CandidateHumanReview(
            "candidate:1", "assessment_semantics", "user:42", "approve", CLOSED, "事实核对完成"
        ),
        **changes,
    )


def append(values, prior=(), candidates=None, **changes):
    options = dict(
        status="awaiting_review",
        closed_at=CLOSED,
        candidates=(candidate(),) if candidates is None else candidates,
        prior=prior,
        values=values,
        gate_policy_version="v2",
    )
    return add_human_reviews(**{**options, **changes})


def contradiction(**changes):
    return replace(
        SemanticContradictionReview(
            CONTRADICTION_POLICY,
            "semantic:1",
            candidate().semantic.output_fingerprint,
            1,
            "原始判定",
            "量表结果仅供参考",
            "该句明确保留了限制说明",
        ),
        **changes,
    )


def test_reviews_append_with_distinct_people_and_do_not_publish_or_change_evidence():
    target = candidate()
    first = review()
    second = review(role="safety_product", reviewer="user:43", decision="reject")
    prior = (first,)
    assert append((second,), prior, (target,)) == (first, second)
    assert prior == (first,)
    assert target == candidate()


@pytest.mark.parametrize(
    "value",
    [
        review(reviewer="user:43"),
        review(role="safety_product"),
        review(candidate_id="candidate:missing"),
        review(reviewed_at=CLOSED - timedelta(microseconds=1)),
    ],
)
def test_batch_failure_preserves_existing_and_does_not_accept_partial_reviews(value):
    first = review(candidate_id="candidate:2")
    prior = (review(),)
    with pytest.raises(ValueError):
        append((first, value), prior, (candidate(), candidate("candidate:2")))
    assert prior == (review(),)


def test_one_person_can_review_different_candidates_but_not_both_roles_of_one():
    values = (review(), review(candidate_id="candidate:2"))
    assert append(values, candidates=(candidate(), candidate("candidate:2"))) == values
    with pytest.raises(ValueError):
        append((review(), review(role="safety_product")))


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "collecting"},
        {"status": "approved"},
        {"status": "blocked"},
        {"closed_at": AT},
        {"closed_at": CLOSED.replace(tzinfo=None)},
        {"candidates": ()},
        {"candidates": (candidate(), candidate())},
        {"values": ()},
        {"prior": [review()]},
    ],
)
def test_review_requires_closed_immutable_evidence(changes):
    arguments = {"values": (review(),), **changes}
    with pytest.raises(ValueError):
        append(**arguments)


@pytest.mark.parametrize(
    "changes",
    [
        {"reason": " "},
        {"reason": "汉" * 334},
        {"reason": "<script>"},
        {"reviewer": ""},
        {"role": "admin"},
        {"decision": "waive"},
        {"reviewed_at": AT.replace(tzinfo=None)},
    ],
)
def test_invalid_review_audit_rejected(changes):
    with pytest.raises(ValueError):
        review(**changes)


def test_semantic_contradiction_review_retains_original_failed_evidence():
    target = candidate()
    value = review(semantic_review=contradiction())
    assert append((value,), candidates=(target,)) == (value,)
    assert target.semantic_assertions[0].status == "failed"
    assert target.semantic.normalized_output == candidate().semantic.normalized_output


@pytest.mark.parametrize(
    "changes",
    [
        {"execution_id": "semantic:other"},
        {"output_fingerprint": "sha256:" + "a" * 64},
        {"assertion_ordinal": 2},
        {"original_detail": "另一判定"},
        {"candidate_excerpt": "原文没有这句话"},
    ],
)
def test_contradiction_review_must_bind_exact_execution_output_assertion_and_excerpt(changes):
    with pytest.raises(ValueError):
        append((review(semantic_review=contradiction(**changes)),))


def test_contradiction_requires_v2_approval_and_original_failed_source_in_both_places():
    value = review(semantic_review=contradiction())
    with pytest.raises(ValueError):
        append((value,), gate_policy_version="v1")
    with pytest.raises(ValueError):
        append((replace(value, decision="reject"),))
    target = candidate()
    for changed in (
        replace(target.semantic_assertions[0], status="passed"),
        replace(target.semantic_assertions[0], scope="case"),
        replace(target.semantic_assertions[0], type="no_unprovided_fact"),
    ):
        with pytest.raises(ValueError):
            append((value,), candidates=(replace(target, semantic_assertions=(changed,)),))
    raw = json.dumps({"decisions": []}).encode()
    changed = replace(target, semantic=replace(target.semantic, normalized_output=raw))
    bound = review(
        semantic_review=contradiction(output_fingerprint=changed.semantic.output_fingerprint)
    )
    with pytest.raises(ValueError):
        append((bound,), candidates=(changed,))


def test_candidate_output_cannot_be_replaced_under_old_semantic_evidence():
    with pytest.raises(ValueError):
        replace(candidate(), normalized_output=b'{"interpretation":"different"}')


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "passed"},
        {"evaluator": "another-judge/v2"},
        {"scope": "case"},
        {"detail": "不同的判断"},
    ],
)
def test_contradiction_cannot_override_an_unmatched_final_candidate_assertion(changes):
    target = candidate()
    changed = replace(target, assertions=(replace(target.assertions[0], **changes),))
    with pytest.raises(ValueError):
        append((review(semantic_review=contradiction()),), candidates=(changed,))
