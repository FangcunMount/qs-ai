import copy
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime

import pytest

from qs_ai.infrastructure.qs_server.evaluation_input import (
    validate_suite_input,
    validate_suite_inputs,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    PUBLISHED_INPUT_VERSION,
    V6,
    V6_PUBLISHED,
    load_suite,
)
from qs_ai.infrastructure.qs_server.preflight import run_preflight
from tests.evaluation_helpers import prepare_evaluation_case
from tests.test_evaluation_case import release


def test_derived_suite_keeps_original_case_facts_assertions_and_profile():
    legacy, current = load_suite(V6), load_suite(V6_PUBLISHED)
    old, new = json.loads(legacy.definition_json), json.loads(current.definition_json)
    assert new.pop("derived_from") == asdict(V6)
    assert new.pop("input_contract") == {
        "construction_version": PUBLISHED_INPUT_VERSION,
        "schema": asdict(current.input_schema),
    }
    new.update(suite_id=V6.id, suite_version=V6.version)
    assert new == old
    assert legacy.input_construction_version is None
    assert current.slots() == legacy.slots()
    assert V6_PUBLISHED.fingerprint != V6.fingerprint
    validate_suite_inputs(current, current.input_schema)


@pytest.mark.parametrize("index", range(1, 8))
def test_versioned_suite_prepares_the_same_case_under_explicit_schema(index):
    suite = load_suite(V6_PUBLISHED)
    bound = replace(release(), suite=V6_PUBLISHED, input_schema=suite.input_schema)
    case_id = f"PROMPT-EVAL-{index:03}"
    # Existing V6 data already uses empty arrays. Only its new contract identity
    # changes, not the Prompt or synthetic case facts sent to the model.
    prepared = prepare_evaluation_case(bound, case_id)
    original = next(
        c for c in json.loads(load_suite(V6).definition_json)["cases"] if c["case_id"] == case_id
    )
    assert json.loads(prepared.assembled_input.provider_payload) == original["provider_payload"]
    with pytest.raises(ValueError, match="input contract"):
        prepare_evaluation_case(replace(bound, suite=V6), case_id)


def test_preflight_still_rejects_without_a_provider_call():
    receipt = run_preflight(V6_PUBLISHED, datetime.now(UTC))
    assert receipt.status == "passed"
    assert receipt.provider_call_count == 0


@pytest.mark.parametrize("damage", ["schema", "null", "source", "version"])
def test_new_input_contract_rejects_schema_payload_and_version_drift(damage):
    suite = load_suite(V6_PUBLISHED)
    schema = suite.input_schema
    payload = copy.deepcopy(json.loads(suite.definition_json)["cases"][3]["provider_payload"])
    if damage == "schema":
        schema = replace(schema, fingerprint="sha256:" + "a" * 64)
    elif damage == "null":
        payload["facts"]["dimensions"][0]["standard_suggestion_refs"] = None
    elif damage == "source":
        payload["source"] = {"report_id": "invented"}
    else:
        suite = replace(suite, input_construction_version="other")
    with pytest.raises(ValueError):
        validate_suite_input(suite, schema, payload)


def test_new_suite_cannot_render_with_legacy_placeholder_schema_reference():
    with pytest.raises(ValueError, match="input contract"):
        prepare_evaluation_case(
            replace(
                release(),
                input_schema=replace(release().input_schema, fingerprint="sha256:" + "a" * 64),
            ),
            "PROMPT-EVAL-001",
        )
