"""Frozen MBTI execution: synthetic contract data, no provider or quality claims."""

import json
from dataclasses import asdict, replace

import pytest
from pydantic import TypeAdapter

from qs_ai.application.execution.generation import DurableGeneration, FrozenGeneration
from qs_ai.application.interpretation.input import MBTIInputPolicy
from qs_ai.application.interpretation.ports import Claim
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.application.interpretation.provider import ModelCall, ModelResponse, ProviderFailure
from qs_ai.application.interpretation.selection import report_selector
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.interpretation.model import EvidenceSet, Fact, RuleViolation
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from tests.test_deepseek_request import prepared as scale_prepared
from tests.test_deepseek_request import route, schema
from tests.test_input_binding import bound_case
from tests.test_mbti_contract import profile, snapshot


def mbti_case():
    session, evidence, _ = bound_case()
    value = snapshot()
    item = replace(
        evidence.items[0],
        report_id=value["source"]["report_id"],
        source_version=f"{value['source']['content_schema_version']}:{value['source']['outcome_id']}",
        facts=(Fact("standard_report", json.dumps(value)),),
    )
    evidence = EvidenceSet(evidence.id, session.id, fingerprint([asdict(item)]), (item,))
    session = replace(session, workflow_version="qs-published-snapshot-v2")
    release = decode_published_profile(profile())
    # Synthetic Profile inherits the fixed message format solely to test runtime binding.
    package = load_prompt(release.render_policy.template_id, release.render_policy.version)
    prepared = prepare_explanation(session, evidence, release, package)
    return (
        Claim("job", "run", session, 1, None, False, None),
        evidence,
        FrozenGeneration(prepared, route(), schema(), publication_id="synthetic-publication"),
    )


def test_mbti_frozen_request_retains_policy_and_byte_roundtrip():
    claim, evidence, request = mbti_case()
    selector = report_selector(claim.session, evidence)
    assert selector.admission_candidates() == (selector,)
    codec = JSONModelCallCodec()
    raw = codec.encode_request(request)
    restored = codec.decode_request(raw)
    assert isinstance(restored.prepared.release.input_policy, MBTIInputPolicy)
    assert restored == request
    assert codec.encode_request(restored) == raw
    doc = json.loads(restored.prepared.assembled_input.canonical_json)
    assert doc["facts"]["model_result"]["type_code"] == "ISFJ"


@pytest.mark.parametrize(
    "field,value", [("scene_contract_version", "other/v1"), ("max_dimensions", 3)]
)
def test_mbti_receipt_policy_cannot_diverge_from_frozen_profile(field, value):
    _, _, request = mbti_case()
    codec = JSONModelCallCodec()
    document = json.loads(codec.encode_request(request))
    document["prepared"]["release"]["input_policy"][field] = value
    with pytest.raises(ValueError):
        codec.decode_request(json.dumps(document))


def test_scale_request_original_bytes_are_preserved():
    request = FrozenGeneration(scale_prepared(), route(), schema())
    codec = JSONModelCallCodec()
    original = TypeAdapter(FrozenGeneration).dump_json(request).decode()
    assert codec.encode_request(request) == original
    assert codec.encode_request(codec.decode_request(original)) == original


def test_mbti_snapshot_cannot_be_rebound_to_scale_workflow():
    claim, evidence, request = mbti_case()
    session = replace(claim.session, workflow_version="qs-published-snapshot-v1")
    with pytest.raises(RuleViolation, match="report_selection_invalid"):
        report_selector(session, evidence)
    release = request.prepared.release
    with pytest.raises(RuleViolation, match="input_workflow_mismatch"):
        prepare_explanation(
            session,
            evidence,
            release,
            load_prompt(release.render_policy.template_id, release.render_policy.version),
        )


@pytest.mark.parametrize("status", ["response_received", "dispatched", "unknown"])
async def test_mbti_recovery_uses_receipt_and_never_calls_gateway(status):
    claim, _, request = mbti_case()
    codec = JSONModelCallCodec()
    response = ModelResponse(
        "invocation", "provider-receipt", route().model, "{}", "{}", "none", 1, 2, 3
    )
    call = ModelCall(
        "invocation",
        status,
        codec.encode_request(request),
        codec.encode_response(response) if status == "response_received" else None,
        None,
    )

    class ExistingCall:
        async def begin_model_call(self, *args):
            return call, False

    class ForbiddenGateway:
        async def generate(self, *args):
            pytest.fail("Recovery must not resolve a current binding or dispatch again")

    execution = DurableGeneration(ExistingCall(), ForbiddenGateway(), codec)
    if status == "response_received":
        result = await execution.execute(claim, request)
        assert result.request == request
        assert result.response == response
    else:
        with pytest.raises(ProviderFailure, match="provider_result_unknown") as caught:
            await execution.execute(claim, request)
        assert caught.value.result_unknown


def mbti_output():
    from tests.test_output_validation import candidate

    value = candidate()
    value["summary"] = "本次测评呈现 ISFJ 偏好组合。"
    for entry in (*value["integrated_insights"], *value["suggestions"]):
        for index, ref in enumerate(entry["evidence_refs"]):
            ref["ref"] = ("dimension:EI", "dimension:SN")[index]
    return value


def test_mbti_artifact_uses_same_envelope_and_replays_identically():
    from qs_ai.application.execution.artifact import build_artifact
    from qs_ai.application.execution.generation import GeneratedExplanation
    from qs_ai.infrastructure.qs_server.output import QSOutputParser

    claim, evidence, request = mbti_case()
    raw = json.dumps(mbti_output(), ensure_ascii=False)
    generated = GeneratedExplanation(
        request,
        ModelResponse(
            "invocation",
            "receipt",
            route().model,
            raw,
            raw,
            "none",
            1,
            2,
            3,
        ),
    )
    result = build_artifact(claim, evidence, generated, QSOutputParser())
    restored = replace(
        generated,
        request=JSONModelCallCodec().decode_request(JSONModelCallCodec().encode_request(request)),
    )
    assert build_artifact(claim, evidence, restored, QSOutputParser()) == result
    assert json.loads(result.content_json)["schema_version"] == "ai-explanation-output/v1"
    assert result.output_validator_version == "qs-ai-output-mbti-single-assessment/v1"
    assert result.report_id == evidence.items[0].report_id


@pytest.mark.parametrize("code", ["INTJ", "ENTP", "intj"])
def test_other_personality_type_cannot_be_delivered(code):
    from qs_ai.application.interpretation.output import InvalidOutput, validate_output
    from qs_ai.infrastructure.qs_server.output import QSOutputParser

    _, _, request = mbti_case()
    value = mbti_output()
    value["summary"] = f"本次测评呈现 {code} 偏好组合。"
    with pytest.raises(InvalidOutput, match="mbti_type_conflict"):
        validate_output(json.dumps(value), request.prepared, QSOutputParser())


def test_model_result_cannot_replace_two_dimension_references():
    from qs_ai.application.interpretation.output import InvalidOutput, validate_output
    from qs_ai.infrastructure.qs_server.output import QSOutputParser

    _, _, request = mbti_case()
    value = mbti_output()
    value["integrated_insights"][0]["evidence_refs"][1] = {
        "kind": "model_result",
        "ref": "model_result",
    }
    with pytest.raises(InvalidOutput, match="dimension_count_outside_policy"):
        validate_output(json.dumps(value), request.prepared, QSOutputParser())


def test_external_admission_selects_only_known_snapshot_decoder():
    from qs_ai.application.interpretation.service import published_workflow_version

    _, evidence, _ = mbti_case()
    assert published_workflow_version(evidence.items) == "qs-published-snapshot-v2"
    _, scale, _ = bound_case()
    assert published_workflow_version(scale.items) == "qs-published-snapshot-v1"
    # Malformed/unsupported inputs still reach original strict source rejection;
    # a version hint cannot turn them into a supported scene.
    for raw in ("{", "null", "[]", '{"schema_version":"qs-report-snapshot/v3"}'):
        bad = (replace(evidence.items[0], facts=(Fact("standard_report", raw),)),)
        assert published_workflow_version(bad) == "qs-published-snapshot-v1"
