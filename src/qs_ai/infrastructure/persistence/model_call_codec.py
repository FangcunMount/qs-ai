import json
from dataclasses import fields, replace

from pydantic import TypeAdapter

from qs_ai.application.execution.generation import FrozenGeneration
from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute

_REQUEST = TypeAdapter(FrozenGeneration)
_RESPONSE = TypeAdapter(ModelResponse)


class JSONModelCallCodec:
    def encode_request(self, request: FrozenGeneration) -> str:
        return _REQUEST.dump_json(request).decode()

    def decode_request(self, raw: str) -> FrozenGeneration:
        # Validate the explicit version separately: a failed union branch must
        # never silently discard v2 fields and reinterpret a corrupt route as v1.
        document = json.loads(raw)
        value = document["route"]
        kind = ModelRouteV2 if "format_version" in value else ModelRoute
        if not isinstance(value, dict) or set(value) - {f.name for f in fields(kind)}:
            raise ValueError("Unexpected frozen route fields")
        route = TypeAdapter(kind).validate_json(json.dumps(value), strict=True)
        return replace(_REQUEST.validate_json(raw, strict=True), route=route)

    def encode_response(self, response: ModelResponse) -> str:
        return _RESPONSE.dump_json(response).decode()

    def decode_response(self, raw: str) -> ModelResponse:
        return _RESPONSE.validate_json(raw, strict=True)
