"""Resolve frozen execution bindings without consulting mutable model eligibility."""

from dataclasses import replace
from typing import Any

import httpx
from langchain_core.messages import HumanMessage, SystemMessage
from langsmith import tracing_context

from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.preparation import PreparedExplanation
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import (
    ModelExecutionIdentity,
    ModelResponse,
    ModelRoute,
    ProviderFailure,
)
from qs_ai.config import Settings
from qs_ai.infrastructure.models.zhipu import ZhipuChatModel, build_chat
from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses


class ModelGatewayRouter:
    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        self._client, self._settings = client, settings

    async def generate(
        self,
        prepared: PreparedExplanation,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse:
        if route.route != prepared.release.provider_route:
            raise ValueError("Provider route does not match published Profile")
        return await self.generate_messages(prepared.messages, route, schema, invocation_id)

    async def generate_messages(
        self,
        messages: PromptMessages,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse:
        endpoint: str | None
        settings = self._settings
        if isinstance(route, ModelRouteV2):
            binding = next(
                (
                    v
                    for v in settings.models.bindings
                    if (v.binding_id, v.revision) == (route.binding_id, route.binding_revision)
                ),
                None,
            )
            if binding is None or not binding.enabled:
                raise ProviderFailure("binding_unavailable")
            if (binding.provider, binding.protocol, binding.adapter_contract) != (
                route.provider,
                route.protocol,
                route.adapter_contract,
            ):
                raise ProviderFailure("model_route_unsupported")
            endpoint = binding.endpoint
        else:
            if route.provider != "deepseek" or route.protocol != "responses":
                raise ProviderFailure("model_route_unsupported")
            endpoint = settings.generation.endpoint
        credential = (
            settings.effective_deepseek_api_key
            if route.provider == "deepseek"
            else settings.zhipu_api_key
        )
        if not endpoint or credential is None or not credential.get_secret_value().strip():
            raise ProviderFailure("binding_unavailable")
        if route.provider == "deepseek":
            response = await DeepSeekResponses(
                self._client, endpoint, credential.get_secret_value()
            ).generate_messages(messages, route, schema, invocation_id)
            return self._with_identity(response, route)
        if not isinstance(route, ModelRouteV2):
            raise ProviderFailure("model_route_unsupported")
        body = build_chat(messages, route, schema, invocation_id)
        model = ZhipuChatModel(
            client=self._client,
            endpoint=endpoint,
            api_key=credential.get_secret_value(),
            limit=2_097_152,
        )
        with tracing_context(enabled=False):
            reply = await model.ainvoke(
                [
                    SystemMessage(content=body["messages"][0]["content"]),
                    HumanMessage(content=body["messages"][1]["content"]),
                ],
                config={"callbacks": []},
                body=body,
                route=route,
                invocation_id=invocation_id,
            )
        return self._with_identity(ModelResponse(**reply.response_metadata["qs_receipt"]), route)

    @staticmethod
    def _with_identity(response: ModelResponse, route: ModelRoute) -> ModelResponse:
        if not isinstance(route, ModelRouteV2):
            return response
        return replace(
            response,
            execution_identity=ModelExecutionIdentity(
                provider=route.provider,
                requested_model=route.model,
                protocol=route.protocol,
                adapter_contract=route.adapter_contract,
                binding_id=route.binding_id,
                binding_revision=route.binding_revision,
                route_fingerprint=route.fingerprint(),
            ),
        )
