"""Synthetic v2 contract vectors, not a production export or quality approval."""

import hashlib
import json
import shutil
from dataclasses import replace
from itertools import product
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.input import InvalidInput, assemble_input
from qs_ai.application.interpretation.release import InvalidRelease
from qs_ai.application.interpretation.selection import snapshot_selector
from qs_ai.domain.governance.publication import (
    PublicationPointer,
    ReleaseSelector,
    resolve_publication,
)
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.profiles import canonical_definition, decode_published_profile
from tests.test_profiles import envelope


def snapshot():
    return json.loads(
        (Path(__file__).parent / "fixtures/personality_contract/snapshot.json").read_text()
    )


def profile():
    entry = envelope()
    d = entry["definition"]
    d.update(
        schema_version="ai-explanation-profile/v2",
        profile_id="participant-mbti-test",
        version="v1",
        scene_contract_version="mbti-single-assessment/v1",
    )
    d["selector"] = dict(
        audience="participant",
        model_kind="typology",
        decision_kind="pole_composition",
        model_code="MBTI_OEJTS",
        model_version="v64-report-202608-v1",
    )
    d["eligibility"].update(
        min_eligible_dimensions=4,
        max_input_dimensions=4,
        eligible_dimension_codes=["EI", "SN", "TF", "JP"],
        excluded_dimension_codes=[],
    )
    d["input_policy"].update(include_norm_context=False, include_model_result=True)
    d["generation_policy"]["input_schema_version"] = "ai-explanation-input/v2"
    return sign(entry)


def sign(entry):
    entry["fingerprint"] = (
        "sha256:" + hashlib.sha256(canonical_definition(entry["definition"]).encode()).hexdigest()
    )
    return entry


def assemble(value, policy=None):
    return assemble_input(
        json.dumps(value, ensure_ascii=False),
        policy or decode_published_profile(profile()).input_policy,
    )


def change(value, path, new):
    target = value
    for key in path[:-1]:
        target = target[key]
    if new is ...:
        del target[path[-1]]
    else:
        target[path[-1]] = new


def test_exact_facts_and_private_provenance_are_preserved():
    value = snapshot()
    result = assemble(value)
    doc = json.loads(result.canonical_json)
    assert doc["schema_version"] == "ai-explanation-input/v2"
    assert doc["scene_contract_version"] == "mbti-single-assessment/v1"
    assert doc["source"] == value["source"]
    Draft202012Validator(load_input_schema(version="ai-explanation-input/v2")).validate(doc)
    assert doc["facts"]["model_result"]["type_code"] == "ISFJ"
    assert [d["pole_facts"]["strength"] for d in doc["facts"]["dimensions"]] == [
        56.25,
        6.25,
        25,
        75,
    ]
    for source, projected in zip(value["dimensions"], doc["facts"]["dimensions"], strict=True):
        assert source["pole_facts"] == projected["pole_facts"]
        assert projected["strength_semantics"] == "preference_strength_not_confidence"
        assert projected["ref"] == "dimension:" + source["code"]
    assert set(json.loads(result.provider_payload)) == {"context", "facts"}
    assert (
        result.fingerprint == "sha256:" + hashlib.sha256(result.canonical_json.encode()).hexdigest()
    )
    value["dimensions"].reverse()
    assert assemble(value) == result


@pytest.mark.parametrize("strength", [0, 0.0, 6.25])
def test_known_strength_is_copied_without_rescoring(strength):
    value = snapshot()
    value["dimensions"][0]["pole_facts"]["strength"] = strength
    # QS is authoritative; this layer checks bounds/coherence, never recalculates strength.
    result = json.loads(assemble(value).canonical_json)
    assert result["facts"]["dimensions"][0]["pole_facts"]["strength"] == strength


@pytest.mark.parametrize(
    "path,new",
    [
        (("schema_version",), "qs-report-snapshot/v1"),
        (("schema_version",), "qs-report-snapshot/v3"),
        (("source", "report_id"), "18446744073709551616"),
        (("source", "report_id"), "0"),
        (("source", "outcome_id"), 101),
        (("source", "report_type"), "draft"),
        (("source", "generated_at"), "2026-09-22T00:00:00"),
        (("source", "generated_at"), "invalid"),
        (("model", "version"), "latest"),
        (("model", "code"), "MBTI"),
        (("model", "algorithm"), "sum"),
        (("runtime", "decision_kind"), "score_range"),
        (("answers",), ["must never reach model"]),
        (("dimensions", 0, "raw_score"), True),
        (("dimensions", 0, "raw_score"), 41),
        (("dimensions", 0, "raw_score"), 7),
        (("dimensions", 0, "raw_score"), float("nan")),
        (("dimensions", 0, "pole_facts"), None),
        (("dimensions", 0, "pole_facts", "strength"), ...),
        (("dimensions", 0, "pole_facts", "strength"), None),
        (("dimensions", 0, "pole_facts", "strength"), True),
        (("dimensions", 0, "pole_facts", "strength"), "56.25"),
        (("dimensions", 0, "pole_facts", "strength"), float("inf")),
        (("dimensions", 0, "pole_facts", "strength"), 101),
        (("dimensions", 0, "pole_facts", "min_score"), 0),
        (("dimensions", 0, "pole_facts", "threshold"), 20),
        (("dimensions", 0, "pole_facts", "left_pole"), "E"),
        (("dimensions", 0, "pole_facts", "preference"), "N"),
        (("dimensions", 0, "pole_facts", "composition_order"), 2),
        (("dimensions", 0, "pole_facts", "composition_order"), True),
        (("model_extra", "type_code"), "INTJ"),
        (("model_extra", "match_percent"), None),
        (("model_extra", "rarity"), 0.1),
        (("dimensions", 0, "description"), "<secret-participant>"),
    ],
)
def test_invalid_report_rejected_without_sensitive_exception(path, new):
    value = snapshot()
    change(value, path, new)
    with pytest.raises(InvalidInput) as caught:
        assemble(value)
    assert str(caught.value) == "Invalid MBTI report snapshot"
    assert "secret-participant" not in str(caught.value)


def test_incomplete_duplicate_axes_and_suggestions_rejected():
    for mode in ("missing", "duplicate", "suggestion"):
        value = snapshot()
        if mode == "missing":
            value["dimensions"].pop()
        elif mode == "duplicate":
            value["dimensions"][1] = value["dimensions"][0]
        else:
            value["suggestions"].append(value["suggestions"][0])
        with pytest.raises(InvalidInput):
            assemble(value)


@pytest.mark.parametrize(
    "path,new",
    [
        (("schema_version",), "ai-explanation-profile/v1"),
        (("scene_contract_version",), "mbti-single-assessment/v2"),
        (("selector", "model_code"), None),
        (("selector", "model_version"), None),
        (("eligibility", "min_eligible_dimensions"), 2),
        (("eligibility", "excluded_dimension_codes"), ["EI"]),
        (("input_policy", "include_norm_context"), True),
        (("input_policy", "include_model_result"), False),
        (("generation_policy", "input_schema_version"), "ai-explanation-input/v1"),
        (("generation_policy", "output_schema_version"), "ai-explanation-output/v2"),
        (("insight_policy", "min_dimension_refs_per_item"), 1),
    ],
)
def test_profile_contract_is_frozen_not_inferred(path, new):
    entry = profile()
    change(entry["definition"], path, new)
    with pytest.raises(InvalidRelease):
        decode_published_profile(sign(entry))


def test_selector_cannot_fall_back_to_scale_or_wildcard_typology():
    selector = snapshot_selector(snapshot())
    scale = ReleaseSelector("participant", "scale", "score_range")
    assert not scale.matches(selector)
    assert selector.key() != scale.key()
    assert resolve_publication([PublicationPointer(scale)], selector) is None
    assert resolve_publication([PublicationPointer(selector)], selector) is None
    for changes in ({"model_code": None}, {"model_version": None}, {"model_version": "v65"}):
        with pytest.raises(ValueError):
            replace(selector, **changes)
    value = snapshot()
    value["schema_version"] = "qs-report-snapshot/v1"
    with pytest.raises(ValueError):
        snapshot_selector(value)


def test_scale_policy_cannot_consume_personality_and_duplicate_keys_fail():
    with pytest.raises(InvalidInput):
        assemble(snapshot(), decode_published_profile(envelope()).input_policy)
    raw = json.dumps(snapshot()).replace('"strength": 56.25', '"strength": 56.25, "strength": 0')
    with pytest.raises(InvalidInput, match="Duplicate"):
        assemble_input(raw, decode_published_profile(profile()).input_policy)


def test_profile_digest_tampering_fails():
    entry = profile()
    entry["definition"]["generation_policy"]["max_output_characters"] += 1
    with pytest.raises(InvalidRelease):
        decode_published_profile(entry)


@pytest.mark.parametrize("letters", list(product("IE", "SN", "FT", "JP")))
def test_all_sixteen_authoritative_types_supported(letters):
    value = snapshot()
    value["model_extra"]["type_code"] = "".join(letters)
    for d, letter in zip(value["dimensions"], letters, strict=True):
        d["pole_facts"]["preference"] = letter
    assert json.loads(assemble(value).canonical_json)["facts"]["model_result"][
        "type_code"
    ] == "".join(letters)


def test_actual_zero_strength_boundary_and_focus_are_preserved():
    value = snapshot()
    value["dimensions"][0]["raw_score"] = 24
    value["dimensions"][0]["pole_facts"]["strength"] = 0
    policy = decode_published_profile(profile()).input_policy
    output = assemble_input(json.dumps(value), policy, focus_areas=("sleep_routine",))
    doc = json.loads(output.canonical_json)
    assert doc["facts"]["dimensions"][0]["pole_facts"]["preference"] == "I"
    assert doc["facts"]["dimensions"][0]["pole_facts"]["strength"] == 0
    assert doc["context"]["focus_areas"] == ["sleep_routine"]
    for kwargs in (
        {"locale": "<invalid>"},
        {"focus_areas": ("unknown",)},
        {"focus_areas": ("sleep_routine", "sleep_routine")},
    ):
        with pytest.raises(InvalidInput):
            assemble_input(json.dumps(value), policy, **kwargs)


@pytest.mark.parametrize("mode", ["tamper", "missing", "unknown_version"])
def test_schema_reading_never_falls_back_to_v1(tmp_path, mode):
    shutil.copytree(schema_directory(), tmp_path, dirs_exist_ok=True)
    target = tmp_path / "ai-explanation-input-v2.schema.json"
    version = "ai-explanation-input/v2"
    if mode == "tamper":
        target.write_bytes(target.read_bytes() + b" ")
    elif mode == "missing":
        target.unlink()
    else:
        version = "latest"
    with pytest.raises(ValueError, match="Cannot load verified input schema"):
        load_input_schema(directory=tmp_path, version=version)
    assert (
        load_input_schema(directory=tmp_path)["properties"]["schema_version"]["const"]
        == "ai-explanation-input/v1"
    )
