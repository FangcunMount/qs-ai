import json
from dataclasses import replace

from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.infrastructure.models.router import ModelGatewayRouter
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from tests.test_model_route_v2 import route


def test_v2_receipt_keeps_original_binding_and_unknown_usage():
    response = ModelResponse("invocation", "request", "glm-5.3", "{}", "{}", "none", None, None, 12)
    enriched = ModelGatewayRouter._with_identity(response, route())
    codec = JSONModelCallCodec()
    restored = codec.decode_response(codec.encode_response(enriched))
    assert restored == enriched
    assert restored.input_tokens is None
    assert restored.execution_identity.route_fingerprint == route().fingerprint()
    assert restored.execution_identity.binding_id == route().binding_id
    assert "endpoint" not in codec.encode_response(enriched)


def test_legacy_receipt_serialization_does_not_gain_null_identity():
    response = ModelResponse("invocation", "request", "old", "{}", "{}", "none", None, None, 12)
    codec = JSONModelCallCodec()
    raw = codec.encode_response(response)
    assert "execution_identity" not in json.loads(raw)
    assert codec.decode_response(raw) == response
    assert codec.encode_response(replace(response)) == raw
