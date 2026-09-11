from pydantic import TypeAdapter

from qs_ai.application.execution.generation import FrozenGeneration
from qs_ai.application.interpretation.provider import ModelResponse

_REQUEST = TypeAdapter(FrozenGeneration)
_RESPONSE = TypeAdapter(ModelResponse)


class JSONModelCallCodec:
    def encode_request(self, request: FrozenGeneration) -> str:
        return _REQUEST.dump_json(request).decode()

    def decode_request(self, raw: str) -> FrozenGeneration:
        return _REQUEST.validate_json(raw, strict=True)

    def encode_response(self, response: ModelResponse) -> str:
        return _RESPONSE.dump_json(response).decode()

    def decode_response(self, raw: str) -> ModelResponse:
        return _RESPONSE.validate_json(raw, strict=True)
