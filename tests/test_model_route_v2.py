import json
from dataclasses import asdict, replace

import pytest

from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.qs_server.routes import load_route


def route():
    return ModelRouteV2(
        route="multi-generation",
        revision="v1",
        provider="zhipu",
        model="glm-5.3",
        protocol="chat_completions",
        structured_output_mode="json_object",
        timeout_milliseconds=180000,
        max_output_tokens=8000,
        reasoning_effort="low",
        model_key="zhipu/quality",
        catalog_revision="20260921-1",
        binding_id="zhipu-official",
        binding_revision="v1",
        adapter_contract="zhipu-chat/v1",
        thinking="enabled",
        temperature=None,
        top_p=None,
    )


def test_v2_asset_roundtrip_and_frozen_identity():
    value = route()
    asset = RouteAsset(value.route, value.revision, value.fingerprint(), value.definition_json())
    assert executable_route(asset) == value
    assert json.loads(value.definition_json())["format_version"] == "qs-ai-model-route/v2"
    assert replace(value, binding_revision="v2").fingerprint() != value.fingerprint()
    assert replace(value, reasoning_effort="high").fingerprint() != value.fingerprint()


def test_v1_roundtrip_keeps_bytes_and_fingerprint():
    original = load_route("balanced_text_v1", "v8")
    assert type(original) is ModelRoute
    asset = RouteAsset(
        original.route, original.revision, original.fingerprint(), original.definition_json()
    )
    result = executable_route(asset)
    assert result == original
    assert result.definition_json() == original.definition_json()
    assert "format_version" not in json.loads(result.definition_json())


@pytest.mark.parametrize(
    "changes",
    [
        {"protocol": "responses"},
        {"adapter_contract": "other/v1"},
        {"idempotent_redispatch": True},
        {"retrieve_by_invocation_id": True},
        {"timeout_milliseconds": 180001},
        {"max_output_tokens": True},
        {"binding_id": ""},
        {"format_version": "future"},
        {"temperature": float("nan")},
    ],
)
def test_v2_invalid_contract_rejected(changes):
    with pytest.raises(ValueError):
        replace(route(), **changes)


def test_v2_cannot_be_downgraded_to_v1():
    value = route()
    payload = asdict(value)
    payload.pop("format_version")
    with pytest.raises((TypeError, ValueError)):
        ModelRoute(**payload)


def test_frozen_request_codec_retains_v2_and_rejects_corruption():
    from qs_ai.application.execution.generation import FrozenGeneration
    from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
    from tests.test_deepseek_request import prepared, schema

    codec = JSONModelCallCodec()
    value = FrozenGeneration(prepared(), route(), schema())
    raw = codec.encode_request(value)
    restored = codec.decode_request(raw)
    assert type(restored.route) is ModelRouteV2
    assert restored == value
    payload = json.loads(raw)
    payload["route"]["adapter_contract"] = "unknown/v1"
    with pytest.raises(ValueError):
        codec.decode_request(json.dumps(payload))


def test_v1_frozen_request_encoding_does_not_gain_v2_fields():
    from pydantic import TypeAdapter

    from qs_ai.application.execution.generation import FrozenGeneration
    from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
    from tests.test_deepseek_request import prepared, schema

    old = load_route("balanced_text_v1", "v8")
    codec = JSONModelCallCodec()
    value = FrozenGeneration(prepared(), old, schema())
    raw = codec.encode_request(value)
    assert json.loads(raw)["route"] == json.loads(TypeAdapter(ModelRoute).dump_json(old))
    assert type(codec.decode_request(raw).route) is ModelRoute
    assert codec.encode_request(codec.decode_request(raw)) == raw
