"""New Profile/input/frozen receipt compatibility; synthetic content only."""

import copy
import json
from dataclasses import replace
from itertools import product

import pytest
from jsonschema import Draft202012Validator

from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.interpretation.input import MBTIThematicInputPolicy, assemble_input
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.application.interpretation.prompts import InvalidPrompt, render_prompt
from qs_ai.application.interpretation.provider import ModelCall, ModelResponse, ProviderFailure
from qs_ai.application.interpretation.release import InvalidRelease
from qs_ai.domain.governance.scenes import MBTI_AXES
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.test_mbti_contract import profile, sign, snapshot
from tests.test_mbti_references import material
from tests.test_mbti_runtime import mbti_case


def thematic_profile():
    value = profile()
    definition = value["definition"]
    definition.update(
        schema_version="ai-explanation-profile/v3",
        scene_contract_version="mbti-single-assessment/v2",
        reference_material=material(),
    )
    definition["generation_policy"].update(
        input_schema_version="ai-explanation-input/v3",
        output_schema_version="ai-explanation-output/v2",
    )
    return sign(value)


def thematic_case():
    claim, evidence, previous = mbti_case()
    release = decode_published_profile(thematic_profile())
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    prepared = prepare_explanation(claim.session, evidence, release, package)
    output_schema = json.loads(
        (schema_directory() / "ai-explanation-output-v2.schema.json").read_text()
    )
    return claim, evidence, replace(previous, prepared=prepared, schema=output_schema)


@pytest.mark.parametrize("poles", list(product(*(axis[1:] for axis in MBTI_AXES))))
def test_input_keeps_exact_report_facts_and_selects_all_three_topics(poles):
    value = snapshot()
    type_code = "".join(poles)
    value["model_extra"]["type_code"] = type_code
    for dimension, pole in zip(value["dimensions"], poles, strict=True):
        dimension["pole_facts"]["preference"] = pole
    release = decode_published_profile(thematic_profile())
    assert isinstance(release.input_policy, MBTIThematicInputPolicy)
    assembled = assemble_input(json.dumps(value), release.input_policy)
    doc = json.loads(assembled.canonical_json)
    baseline = assemble_input(json.dumps(value), decode_published_profile(profile()).input_policy)
    assert doc["facts"] == json.loads(baseline.canonical_json)["facts"]
    assert doc["source"] == value["source"]
    assert set(json.loads(assembled.provider_payload)) == {"context", "facts", "reference_material"}
    assert (
        doc["reference_material"]
        == release.input_policy.reference_material.select(type_code).projection()
    )
    assert {item["topic"] for item in doc["reference_material"]["entries"]} == {
        "personality",
        "career",
        "relationships",
    }
    Draft202012Validator(load_input_schema(version="ai-explanation-input/v3")).validate(doc)


@pytest.mark.parametrize(
    "path,value",
    [
        (("scene_contract_version",), "mbti-single-assessment/v1"),
        (("generation_policy", "input_schema_version"), "ai-explanation-input/v2"),
        (("generation_policy", "output_schema_version"), "ai-explanation-output/v1"),
        (("selector", "model_version"), "latest"),
        (("input_policy", "include_norm_context"), True),
        (("reference_material", "entries", 0, "source_ids"), ["missing"]),
        (("reference_material", "entries", 0, "pole"), "X"),
        (("reference_material", "approved"), True),
    ],
)
def test_invalid_profile_rejected_even_with_new_fingerprint(path, value):
    entry = thematic_profile()
    node = entry["definition"]
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(InvalidRelease):
        decode_published_profile(sign(entry))


def test_missing_material_does_not_fall_back_to_old_profile_or_files():
    entry = thematic_profile()
    del entry["definition"]["reference_material"]
    with pytest.raises(InvalidRelease):
        decode_published_profile(sign(entry))


def test_reference_edit_changes_profile_and_frozen_input_fingerprints():
    original = thematic_profile()
    changed = copy.deepcopy(original)
    changed["definition"]["reference_material"]["entries"][0]["content"] = "修改后的合成参考"
    changed = sign(changed)
    assert original["fingerprint"] != changed["fingerprint"]
    raw = json.dumps(snapshot())
    before = assemble_input(raw, decode_published_profile(original).input_policy)
    after = assemble_input(raw, decode_published_profile(changed).input_policy)
    assert before.fingerprint != after.fingerprint
    assert (
        json.loads(before.provider_payload)["facts"] == json.loads(after.provider_payload)["facts"]
    )


def test_reference_data_is_only_allowed_in_explicit_new_scene():
    _, _, request = thematic_case()
    release = request.prepared.release
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    raw = request.prepared.assembled_input.provider_payload
    with pytest.raises(InvalidPrompt):
        render_prompt(package, release.render_policy, raw)
    with pytest.raises(InvalidPrompt):
        render_prompt(package, release.render_policy, raw, scene_contract_version="other/v1")
    assert json.loads(request.prepared.messages.data_json) == json.loads(raw)


def test_new_frozen_request_roundtrip_preserves_original_reference_content():
    _, _, request = thematic_case()
    codec = JSONModelCallCodec()
    raw = codec.encode_request(request)
    restored = codec.decode_request(raw)
    assert isinstance(restored.prepared.release.input_policy, MBTIThematicInputPolicy)
    assert restored == request
    assert codec.encode_request(restored) == raw


@pytest.mark.parametrize("target", ["policy", "payload", "canonical", "message", "hash"])
def test_reference_tampering_in_any_frozen_projection_is_rejected(target):
    _, _, request = thematic_case()
    codec = JSONModelCallCodec()
    raw = json.loads(codec.encode_request(request))
    prepared = raw["prepared"]
    if target == "policy":
        prepared["release"]["input_policy"]["reference_material"]["entries"][0]["content"] = (
            "篡改内容"
        )
    elif target == "hash":
        prepared["assembled_input"]["fingerprint"] = "sha256:" + "a" * 64
    else:
        if target == "message":
            owner, field = prepared["messages"], "data_json"
        else:
            owner = prepared["assembled_input"]
            field = "provider_payload" if target == "payload" else "canonical_json"
        value = json.loads(owner[field])
        value["reference_material"]["entries"][0]["content"] = "篡改内容"
        owner[field] = json.dumps(value)
    with pytest.raises(ValueError):
        codec.decode_request(json.dumps(raw))


@pytest.mark.parametrize("status", ["response_received", "dispatched", "unknown"])
async def test_receipt_recovery_never_loads_current_references_or_sends_again(status):
    claim, _, request = thematic_case()
    codec = JSONModelCallCodec()
    response = ModelResponse(
        "invocation", "receipt", request.route.model, "{}", "{}", "none", 1, 2, 3
    )
    call = ModelCall(
        "invocation",
        status,
        codec.encode_request(request),
        codec.encode_response(response) if status == "response_received" else None,
        None,
    )

    class Store:
        async def begin_model_call(self, *args):
            return call, False

    class NoDispatch:
        async def generate(self, *args):
            pytest.fail("Frozen receipt recovery cannot dispatch or resolve new references")

    execution = DurableGeneration(Store(), NoDispatch(), codec)
    if status == "response_received":
        assert (await execution.execute(claim, request)).request == request
    else:
        with pytest.raises(ProviderFailure, match="provider_result_unknown"):
            await execution.execute(claim, request)


@pytest.mark.parametrize("value", [None, [], 1])
def test_corrupt_canonical_json_shape_is_a_classified_receipt_error(value):
    _, _, request = thematic_case()
    codec = JSONModelCallCodec()
    document = json.loads(codec.encode_request(request))
    document["prepared"]["assembled_input"]["canonical_json"] = json.dumps(value)
    with pytest.raises(ValueError):
        codec.decode_request(json.dumps(document))
