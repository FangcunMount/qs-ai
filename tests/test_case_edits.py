import copy
import json

import pytest

from qs_ai.domain.evaluation.case_edits import apply, parse
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite


@pytest.fixture
def cases():
    return json.loads(load_suite(V6_PUBLISHED).definition_json)["cases"]


def edit(case):
    return {
        "case_id": case["case_id"],
        "title": "Revised description",
        "purpose": case["purpose"],
        "provider_payload": copy.deepcopy(case["provider_payload"]),
        "assertions": copy.deepcopy(case["expected"]["assertions"]),
    }


def test_case_revision_preserves_original_and_all_obligations(cases):
    original = copy.deepcopy(cases)
    case = next(c for c in cases if c["stage"] == "generation")
    revised = apply(cases, json.dumps([edit(case)]))
    assert cases == original
    assert len(revised) == len(cases)
    for before, after in zip(cases, revised, strict=True):
        assert before["case_id"] == after["case_id"]
        assert before["expected"] == after["expected"]
    assert (
        next(c for c in revised if c["case_id"] == case["case_id"])["title"]
        == "Revised description"
    )


def test_preflight_cannot_be_changed(cases):
    case = next(c for c in cases if c["stage"] != "generation")
    with pytest.raises(ValueError):
        apply(cases, json.dumps([edit(case)]))


def test_assertion_cannot_be_removed(cases):
    value = edit(next(c for c in cases if c["stage"] == "generation"))
    value["assertions"] = []
    with pytest.raises(ValueError):
        apply(cases, json.dumps([value]))


@pytest.mark.parametrize("raw", ['[{"case_id":"a","case_id":"b"}]', "[NaN]", "[]", "{}"])
def test_invalid_revision_is_rejected(raw):
    with pytest.raises(ValueError):
        parse(raw)


def test_unknown_case_is_rejected(cases):
    value = edit(next(c for c in cases if c["stage"] == "generation"))
    value["case_id"] = "unregistered-case"
    with pytest.raises(ValueError):
        apply(cases, json.dumps([value]))


def test_safety_assertion_parameters_are_immutable():
    cases = [
        {
            "case_id": "safe",
            "stage": "generation",
            "title": "Safety",
            "purpose": "Safety",
            "provider_payload": {},
            "expected": {
                "assertions": [{"type": "forbid_literal_substrings", "values": ["forbidden"]}]
            },
        }
    ]
    value = edit(cases[0])
    value["assertions"][0]["values"] = ["different"]
    with pytest.raises(ValueError, match="Core safety"):
        apply(cases, json.dumps([value]))
