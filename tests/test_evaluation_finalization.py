from dataclasses import replace

import pytest

from qs_ai.domain.evaluation.finalization import finalize_review
from qs_ai.domain.evaluation.quality_gates import evaluate_quality_gates
from tests.test_quality_gates import complete


def test_final_decision_follows_all_quality_gates_after_identity_and_closure_validation():
    inputs = complete()
    quality = evaluate_quality_gates(**inputs)
    assert finalize_review(quality, "user:42", "核对完成").status == "approved"
    inputs["reviews"] = (replace(inputs["reviews"][0], decision="reject"), *inputs["reviews"][1:])
    quality = evaluate_quality_gates(**inputs)
    assert finalize_review(quality, "user:42", "人工拒绝").status == "rejected"


def test_incomplete_reviews_cannot_be_finalized_as_rejection():
    inputs = complete()
    inputs["reviews"] = inputs["reviews"][:-1]
    quality = evaluate_quality_gates(**inputs)
    with pytest.raises(ValueError, match="complete"):
        finalize_review(quality, "user:42", "尚缺一条评审")


@pytest.mark.parametrize("reason", ["", " ", "x" * 1001, "<script>"])
def test_audited_reason_required(reason):
    with pytest.raises(ValueError, match="audit"):
        finalize_review(evaluate_quality_gates(**complete()), "user:42", reason)
