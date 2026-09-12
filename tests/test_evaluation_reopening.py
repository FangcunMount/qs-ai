import copy
import json
from dataclasses import replace
from datetime import timedelta

import pytest

from qs_ai.domain.evaluation.quality_gates import GateReason, evaluate_quality_gates
from qs_ai.domain.evaluation.reopening import (
    eligible_candidate_ids,
    reopen_review,
    validate_reopened_reviews,
    validate_reopening_history,
)
from tests.test_evaluation_review import contradiction
from tests.test_quality_gates import complete


def rejected():
    inputs = complete()
    candidates = list(inputs["candidates"])
    semantics = list(inputs["semantics"])
    for index in (0, 6):
        c = candidates[index].evidence
        assertion = replace(c.assertions[0], status="failed")
        body = json.loads(c.semantic.normalized_output)
        body["decisions"][0].update(status="failed", detail=assertion.detail)
        raw = json.dumps(body).encode()
        semantic = replace(c.semantic, raw_output=raw, normalized_output=raw)
        evidence = replace(
            c,
            semantic=semantic,
            assertions=(assertion, c.assertions[1]),
            semantic_assertions=(assertion,),
        )
        candidates[index] = replace(candidates[index], evidence=evidence)
        semantics[index] = semantic
    inputs.update(candidates=tuple(candidates), semantics=tuple(semantics))
    gate = evaluate_quality_gates(**inputs)
    assert gate.gate_passes == (("G3", True), ("G4", False), ("G5", True))
    return inputs, dict(
        candidates=tuple(c.evidence for c in inputs["candidates"]),
        reviews=inputs["reviews"],
        gate=gate,
        closed_at=inputs["closed_at"],
        status="rejected",
        gate_policy_version="v2",
        reopening_count=0,
    )


def opening(options, **changes):
    return reopen_review(
        **{**options, **changes},
        actor="user:44",
        reason="重新核对语义分歧",
        at=options["gate"].evaluated_at + timedelta(seconds=1),
    )


def test_reopening_keeps_full_round_and_only_releases_affected_signatures():
    _, options = rejected()
    before = copy.deepcopy(options)
    result = opening(options)
    assert result.candidate_ids == ("candidate:0", "candidate:6")
    assert result.previous_gate == options["gate"]
    assert result.previous_reviews == options["reviews"]
    assert len(result.previous_reviews) == 70 and len(result.retained_reviews) == 66
    assert options == before  # Includes original output, all assertions and signatures.
    validate_reopened_reviews(result, result.retained_reviews)


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "approved"),
        ("status", "awaiting_review"),
        ("status", "canceled"),
        ("gate_policy_version", "v1"),
        ("reopening_count", 3),
        ("reopening_count", -1),
        ("reopening_count", True),
    ],
)
def test_reopening_never_overrides_terminal_rules_or_round_limit(field, value):
    _, options = rejected()
    assert eligible_candidate_ids(**{**options, field: value}) == ()
    with pytest.raises(ValueError):
        opening(options, **{field: value})


@pytest.mark.parametrize(
    "reason",
    [
        GateReason("G3", "infrastructure_success_rate_below_threshold"),
        GateReason("G4", "candidate_semantic_score_below_minimum"),
        GateReason("G4", "case_assertion_stability_failed"),
        GateReason("G5", "human_review_rejected"),
    ],
)
def test_other_failure_classes_cannot_be_reopened(reason):
    _, options = rejected()
    gate = options["gate"]
    assert (
        eligible_candidate_ids(
            **{**options, "gate": replace(gate, reasons=(*gate.reasons, reason))}
        )
        == ()
    )


@pytest.mark.parametrize(
    "change",
    [
        {"evaluator": "deterministic"},
        {"scope": "case"},
        {"type": "other_hard_rule"},
        {"status": "blocked"},
    ],
)
def test_only_accepted_failed_default_semantic_claims_can_reopen(change):
    _, options = rejected()
    candidates = list(options["candidates"])
    c = candidates[0]
    candidates[0] = replace(c, assertions=(replace(c.assertions[0], **change), c.assertions[1]))
    assert eligible_candidate_ids(**{**options, "candidates": tuple(candidates)}) == ()


def test_failure_must_exist_in_accepted_semantic_assertions():
    _, options = rejected()
    candidates = list(options["candidates"])
    c = candidates[0]
    candidates[0] = replace(c, semantic_assertions=(replace(c.assertions[0], status="passed"),))
    assert eligible_candidate_ids(**{**options, "candidates": tuple(candidates)}) == ()


def test_already_dually_adjudicated_candidate_is_excluded():
    inputs, options = rejected()
    candidate = options["candidates"][0]
    note = contradiction(
        execution_id=candidate.semantic.execution_id,
        output_fingerprint=candidate.semantic.output_fingerprint,
    )
    reviews = tuple(
        replace(r, semantic_review=note) if r.candidate_id == candidate.candidate_id else r
        for r in options["reviews"]
    )
    gate = evaluate_quality_gates(**{**inputs, "reviews": reviews})
    assert eligible_candidate_ids(**{**options, "gate": gate, "reviews": reviews}) == (
        "candidate:6",
    )


def test_new_signatures_cannot_predate_reopening_or_change_unaffected_reviews():
    _, options = rejected()
    result = opening(options)
    with pytest.raises(ValueError, match="predates"):
        validate_reopened_reviews(result, (*result.retained_reviews, options["reviews"][0]))
    with pytest.raises(ValueError, match="Unaffected"):
        validate_reopened_reviews(result, result.retained_reviews[1:])
    changed = replace(result.retained_reviews[0], reason="修改不相关审核")
    with pytest.raises(ValueError, match="Unaffected"):
        validate_reopened_reviews(result, (changed, *result.retained_reviews[1:]))
    new = replace(options["reviews"][0], reviewed_at=result.reopened_at)
    validate_reopened_reviews(result, (*result.retained_reviews, new))


@pytest.mark.parametrize("case", ["reason", "actor", "naive", "early"])
def test_reopening_audit_is_mandatory(case):
    _, options = rejected()
    args = dict(actor="user:44", reason="核对语义", at=options["gate"].evaluated_at)
    if case in ("reason", "actor"):
        args[case] = ""
    elif case == "naive":
        args["at"] = args["at"].replace(tzinfo=None)
    else:
        args["at"] -= timedelta(seconds=1)
    with pytest.raises(ValueError):
        reopen_review(**options, **args)


def test_three_rounds_preserve_all_previous_reviews_and_reject_a_fourth():
    inputs, options = rejected()
    history = ()
    for index in range(3):
        entry = opening(options, reopening_count=index)
        history = (*history, entry)
        validate_reopening_history(
            history,
            entry.retained_reviews,
            options["candidates"],
            options["closed_at"],
            gate_policy_version="v2",
        )
        renewed = tuple(
            replace(r, reviewed_at=entry.reopened_at)
            if r.candidate_id in entry.candidate_ids
            else r
            for r in entry.previous_reviews
        )
        gate = evaluate_quality_gates(
            **{
                **inputs,
                "reviews": renewed,
                "evaluated_at": entry.reopened_at + timedelta(seconds=1),
            }
        )
        options.update(reviews=renewed, gate=gate)
    assert [len(h.previous_reviews) for h in history] == [70, 70, 70]
    with pytest.raises(ValueError):
        opening(options, reopening_count=3)
    with pytest.raises(ValueError, match="three"):
        validate_reopening_history(
            (*history, history[-1]),
            options["reviews"],
            options["candidates"],
            options["closed_at"],
            gate_policy_version="v2",
        )


def test_historical_candidate_selection_cannot_be_rewritten():
    _, options = rejected()
    entry = opening(options)
    altered = replace(entry, candidate_ids=("candidate:0",))
    with pytest.raises(ValueError, match="candidates"):
        validate_reopening_history(
            (altered,),
            altered.retained_reviews,
            options["candidates"],
            options["closed_at"],
            gate_policy_version="v2",
        )
