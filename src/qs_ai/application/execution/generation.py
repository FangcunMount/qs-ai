"""Single-dispatch generation with durable recovery, independent of HTTP/MySQL."""

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from qs_ai.application.interpretation.ports import Claim
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.provider import (
    ModelCall,
    ModelResponse,
    ModelRoute,
    ProviderFailure,
)


class ModelCallStore(Protocol):
    async def begin_model_call(self, claim: Claim, request_json: str) -> tuple[ModelCall, bool]: ...

    async def record_model_response(
        self,
        claim: Claim,
        invocation_id: str,
        *,
        response_json: str | None = None,
        failure_code: str | None = None,
        result_unknown: bool = False,
    ) -> None: ...


class ModelGateway(Protocol):
    async def generate(
        self,
        prepared: PreparedExplanation,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse: ...


@dataclass(frozen=True)
class FrozenGeneration:
    prepared: PreparedExplanation
    route: ModelRoute
    schema: dict[str, Any]
    version: Literal["qs-ai-generation/v1"] = "qs-ai-generation/v1"
    publication_id: str | None = None
    manifest_fingerprint: str | None = None


@dataclass(frozen=True)
class GeneratedExplanation:
    request: FrozenGeneration
    response: ModelResponse


class ModelCallCodec(Protocol):
    def encode_request(self, request: FrozenGeneration) -> str: ...
    def decode_request(self, raw: str) -> FrozenGeneration: ...
    def encode_response(self, response: ModelResponse) -> str: ...
    def decode_response(self, raw: str) -> ModelResponse: ...


class DurableGeneration:
    def __init__(self, store: ModelCallStore, gateway: ModelGateway, codec: ModelCallCodec) -> None:
        self.store = store
        self.gateway = gateway
        self.codec = codec

    async def execute(self, claim: Claim, request: FrozenGeneration) -> GeneratedExplanation:
        # Endpoint and credential are owned by the gateway and never serialized.
        call, created = await self.store.begin_model_call(claim, self.codec.encode_request(request))
        try:
            frozen = self.codec.decode_request(call.request_json)
        except ValueError:
            raise ProviderFailure("model_call_record_invalid") from None
        if request.publication_id is not None and frozen != request:
            raise ProviderFailure("model_call_configuration_mismatch")
        if not created:
            if call.status == "response_received" and call.response_json is not None:
                try:
                    response = self.codec.decode_response(call.response_json)
                except ValueError:
                    raise ProviderFailure("model_call_record_invalid") from None
                self._check_response(call, frozen, response)
                return GeneratedExplanation(frozen, response)
            if call.status in {"dispatched", "unknown"}:
                raise ProviderFailure("provider_result_unknown", result_unknown=True)
            if call.status == "failed" and call.failure_code:
                raise ProviderFailure(call.failure_code)
            raise ProviderFailure("model_call_record_invalid")
        try:
            response = await self.gateway.generate(
                frozen.prepared, frozen.route, frozen.schema, call.invocation_id
            )
            self._check_response(call, frozen, response)
        except ProviderFailure as error:
            await self.store.record_model_response(
                claim,
                call.invocation_id,
                failure_code=error.code,
                result_unknown=error.result_unknown,
            )
            raise
        # Cancellation, lease loss, or a failed commit leave a dispatched marker.
        # A new worker must not infer that no request was sent from a missing receipt.
        await self.store.record_model_response(
            claim, call.invocation_id, response_json=self.codec.encode_response(response)
        )
        return GeneratedExplanation(frozen, response)

    @staticmethod
    def _check_response(
        call: ModelCall, request: FrozenGeneration, response: ModelResponse
    ) -> None:
        if response.invocation_id != call.invocation_id or response.model != request.route.model:
            raise ProviderFailure("model_call_response_mismatch", result_unknown=True)
