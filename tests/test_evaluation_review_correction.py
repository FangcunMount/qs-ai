from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from qs_ai.domain.evaluation.review_correction import (
    ReviewCorrection,
    effective_reviews,
    review_fingerprint,
    validate_corrections,
)
from tests.test_evaluation_review import CLOSED, candidate, review


def correction(old=None, source=17, **changes):
    old = old or review(decision="reject")
    value = ReviewCorrection(
        str(uuid4()),
        source,
        source + 1,
        review_fingerprint(old),
        candidate().semantic.candidate_output_fingerprint,
        old,
        replace(
            old,
            decision="approve",
            reason="标题措辞改进，不阻塞",
            reviewed_at=CLOSED + timedelta(seconds=1),
        ),
    )
    return replace(value, **changes)


def test_original_signature_and_distinct_roles_preserved():
    old = review(decision="reject")
    other = review(role="safety_product", reviewer="user:43")
    entry = correction(old)
    result = validate_corrections(
        (old, other), (entry,), (candidate(),), CLOSED, 18, entry.review.reviewed_at, "v2"
    )
    assert result == (entry.review, other)
    assert old.decision == "reject" and entry.previous_review == old
    assert effective_reviews((old, other), (), 17) == (old, other)


@pytest.mark.parametrize(
    "field,value",
    [
        ("previous_review_fingerprint", "sha256:" + "0" * 64),
        ("command_id", "not-a-command"),
        ("source_version", 0),
        ("version", 17),
        ("review", review(reviewer="user:43")),
        ("review", review(role="safety_product")),
    ],
)
def test_forged_identity_is_rejected(field, value):
    with pytest.raises(ValueError):
        correction(**{field: value})


def test_corrupt_chain_evidence_clock_and_revision_fail_closed():
    old = review(decision="reject")
    entry = correction(old)
    for original, entries, version, at in [
        ((review(),), (entry,), 18, entry.review.reviewed_at),
        ((old,), (entry, entry), 19, entry.review.reviewed_at),
        ((old,), (entry,), 17, entry.review.reviewed_at),
        ((old,), (entry,), 18, CLOSED),
        (
            (old,),
            (replace(entry, candidate_output_fingerprint="sha256:" + "0" * 64),),
            18,
            entry.review.reviewed_at,
        ),
    ]:
        with pytest.raises(ValueError):
            validate_corrections(original, entries, (candidate(),), CLOSED, version, at, "v2")


def test_correction_chain_is_bounded_and_retains_every_intermediate_signature():
    original = review(decision="reject")
    entries = []
    previous = original
    for n in range(4):
        entry = correction(previous, 17 + n)
        entries.append(entry)
        if n < 3:
            assert effective_reviews((original,), tuple(entries), 18 + n) == (entry.review,)
        previous = entry.review
    with pytest.raises(ValueError, match="limit"):
        effective_reviews((original,), tuple(entries), 21)
