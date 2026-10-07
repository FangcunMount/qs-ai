"""Exploration facts are synthetic; these checks are not candidate quality approval."""

import hashlib
import json
import os
from dataclasses import replace
from itertools import product
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.interpretation.input import InvalidInput, assemble_input
from qs_ai.application.interpretation.provider import ModelCall, ModelResponse, ProviderFailure
from qs_ai.application.interpretation.release import InvalidRelease
from qs_ai.application.interpretation.selection import snapshot_selector
from qs_ai.domain.governance.scenes import (
    MBTI_EXPLORATION_MODEL,
    MBTI_EXPLORATION_VERSION,
    mbti_model_contract,
)
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.qs_server.input_schema import load_input_schema
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.test_mbti_contract import sign, snapshot
from tests.test_mbti_themes_input import thematic_case, thematic_profile


def exploration_profile():
    value = thematic_profile()
    d = value["definition"]
    d["selector"].update(model_code=MBTI_EXPLORATION_MODEL, model_version=MBTI_EXPLORATION_VERSION)
    d["reference_material"].update(
        model_code=MBTI_EXPLORATION_MODEL, model_version=MBTI_EXPLORATION_VERSION
    )
    d["generation_policy"]["input_schema_version"] = "ai-explanation-input/v4"
    return sign(value)


def exploration_snapshot():
    value = snapshot()
    value["model"].update(
        code=MBTI_EXPLORATION_MODEL, version=MBTI_EXPLORATION_VERSION, title="16人格测评（探索版）"
    )
    c = mbti_model_contract(MBTI_EXPLORATION_MODEL, MBTI_EXPLORATION_VERSION)
    for d, axis, bounds in zip(value["dimensions"], c.axes, c.bounds, strict=True):
        d["raw_score"] = 0
        d["pole_facts"].update(
            left_pole=axis[1],
            right_pole=axis[2],
            preference=axis[1],
            min_score=bounds[0],
            max_score=bounds[1],
            threshold=bounds[2],
            strength=0,
        )
    value["model_extra"]["type_code"] = "ESTJ"
    return value


@pytest.mark.parametrize("poles", list(product("EI", "SN", "TF", "JP")))
def test_all_types_keep_exploration_identity_boundaries_and_known_strength(poles):
    value = exploration_snapshot()
    value["model_extra"]["type_code"] = "".join(poles)
    for d, pole in zip(value["dimensions"], poles, strict=True):
        d["pole_facts"]["preference"] = pole
    policy = decode_published_profile(exploration_profile()).input_policy
    assembled = assemble_input(json.dumps(value), policy)
    doc = json.loads(assembled.canonical_json)
    Draft202012Validator(load_input_schema(version="ai-explanation-input/v4")).validate(doc)
    assert doc["schema_version"] == "ai-explanation-input/v4"
    assert doc["reference_material"]["model_code"] == MBTI_EXPLORATION_MODEL
    assert snapshot_selector(value).model_code == MBTI_EXPLORATION_MODEL
    for original, projected in zip(value["dimensions"], doc["facts"]["dimensions"], strict=True):
        assert projected["pole_facts"] == original["pole_facts"]
        assert projected["raw_score"]["value"] == original["raw_score"]
        assert projected["strength_semantics"] == "preference_strength_not_confidence"
    assert len(doc["reference_material"]["entries"]) == 12
    assert len({e["topic"] for e in doc["reference_material"]["entries"]}) == 3
    assert (
        assembled.fingerprint
        == "sha256:" + hashlib.sha256(assembled.canonical_json.encode()).hexdigest()
    )


@pytest.mark.parametrize(
    "change", ["base reference", "base schema", "cross version", "other model"]
)
def test_profile_cannot_cross_bind_models_or_old_contract(change):
    entry = exploration_profile()
    d = entry["definition"]
    if change == "base reference":
        d["reference_material"]["model_code"] = "MBTI_OEJTS"
        d["reference_material"]["model_version"] = "v64-report-202608-v1"
    elif change == "base schema":
        d["generation_policy"]["input_schema_version"] = "ai-explanation-input/v3"
    elif change == "cross version":
        d["selector"]["model_version"] = "v64-report-202608-v1"
    else:
        d["selector"]["model_code"] = "OTHER"
    with pytest.raises(InvalidRelease):
        decode_published_profile(sign(entry))


@pytest.mark.parametrize(
    "change",
    [
        "base report",
        "base policy",
        "unknown version",
        "base poles",
        "base range",
        "JP bound",
        "missing strength",
        "missing poles",
    ],
)
def test_source_incomplete_or_mixed_model_is_rejected_without_fallback(change):
    value = exploration_snapshot()
    policy = decode_published_profile(exploration_profile()).input_policy
    if change == "base report":
        value = snapshot()
    elif change == "base policy":
        policy = decode_published_profile(thematic_profile()).input_policy
    elif change == "unknown version":
        value["model"]["version"] = "v55"
    elif change == "base poles":
        value["dimensions"][0]["pole_facts"].update(left_pole="I", right_pole="E")
    elif change == "base range":
        value["dimensions"][0]["pole_facts"].update(min_score=8, max_score=40, threshold=24)
    elif change == "JP bound":
        value["dimensions"][3]["raw_score"] = 25
    elif change == "missing strength":
        del value["dimensions"][0]["pole_facts"]["strength"]
    else:
        del value["dimensions"][0]["pole_facts"]
    with pytest.raises(InvalidInput):
        assemble_input(json.dumps(value), policy)


def exploration_request():
    claim, evidence, previous = thematic_case()
    policy = decode_published_profile(exploration_profile())
    assembled = assemble_input(json.dumps(exploration_snapshot()), policy.input_policy)
    package = load_prompt(policy.render_policy.template_id, policy.render_policy.version)
    from qs_ai.application.interpretation.prompts import render_prompt

    prepared = replace(
        previous.prepared,
        assembled_input=assembled,
        release=policy,
        messages=render_prompt(
            package,
            policy.render_policy,
            assembled.provider_payload,
            scene_contract_version=policy.input_policy.scene_contract_version,
        ),
    )
    return claim, replace(previous, prepared=prepared)


@pytest.mark.parametrize("status", ["response_received", "dispatched", "unknown"])
async def test_exploration_receipts_replay_original_identity_without_dispatch(status):
    claim, request = exploration_request()
    codec = JSONModelCallCodec()
    raw = codec.encode_request(request)
    assert codec.decode_request(raw) == request
    assert codec.encode_request(codec.decode_request(raw)) == raw
    response = ModelResponse(
        "invocation", "receipt", request.route.model, "{}", "{}", "none", 1, 2, 3
    )
    call = ModelCall(
        "invocation",
        status,
        raw,
        codec.encode_response(response) if status == "response_received" else None,
        None,
    )

    class ExistingCall:
        async def begin_model_call(self, *args):
            return call, False

    class ForbiddenGateway:
        async def generate(self, *args):
            pytest.fail("Original receipt must not dispatch again")

    executor = DurableGeneration(ExistingCall(), ForbiddenGateway(), codec)
    if status == "response_received":
        assert (await executor.execute(claim, request)).request == request
    else:
        with pytest.raises(ProviderFailure, match="provider_result_unknown"):
            await executor.execute(claim, request)


@pytest.mark.interop
def test_real_go_projector_exploration_bytes_use_v4_and_preserve_original_facts():
    path = os.environ.get("QS_AI_SNAPSHOT_VECTOR_OUT")
    if not path:
        pytest.skip("Go-generated vector required")
    value = json.loads(Path(path).read_text())["mbti_exploration"]
    result = assemble_input(
        json.dumps(value), decode_published_profile(exploration_profile()).input_policy
    )
    doc = json.loads(result.canonical_json)
    Draft202012Validator(load_input_schema(version="ai-explanation-input/v4")).validate(doc)
    assert doc["facts"]["model_result"] == value["model_extra"]
    for a, b in zip(value["dimensions"], doc["facts"]["dimensions"], strict=True):
        assert a["pole_facts"] == b["pole_facts"]
