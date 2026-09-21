"""Bounded synthetic connectivity probes; never enable models or publish assets."""

import argparse
import asyncio
import json
from uuid import uuid4

import httpx
from jsonschema import validate

from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.config import Settings
from qs_ai.infrastructure.models.router import ModelGatewayRouter
from qs_ai.model_configuration import ModelBinding, ModelConfiguration

TARGETS = {
    "deepseek/deepseek-v4-pro": ("deepseek", "deepseek-v4-pro", ("generation", "semantic")),
    "deepseek/deepseek-flash": ("deepseek", "deepseek-flash", ("generation",)),
    "zhipu/glm-5.3": ("zhipu", "glm-5.3", ("generation", "semantic")),
    "zhipu/glm-5.3-flash": ("zhipu", "glm-5.3-flash", ("generation",)),
}


def probe_route(key: str, purpose: str) -> tuple[ModelBinding, ModelRouteV2]:
    provider, model, purposes = TARGETS[key]
    if purpose not in purposes:
        raise ValueError("Unsupported probe purpose")
    deepseek = provider == "deepseek"
    binding = ModelBinding(
        binding_id=provider + "-official",
        revision="v1",
        provider=provider,
        protocol="responses" if deepseek else "chat_completions",
        adapter_contract="deepseek-responses/v1" if deepseek else "zhipu-chat/v1",
        endpoint="https://api.deepseek.com/responses"
        if deepseek
        else "https://open.bigmodel.cn/api/paas/v4/chat/completions",
        credential_slot=provider,
    )
    return binding, ModelRouteV2(
        route="connectivity-probe",
        revision="v1",
        provider=provider,
        model=model,
        protocol=binding.protocol,
        adapter_contract=binding.adapter_contract,
        structured_output_mode="json_schema" if deepseek else "json_object",
        timeout_milliseconds=120000 if purpose == "generation" else 180000,
        max_output_tokens=1024,
        reasoning_effort="none" if deepseek and purpose == "generation" else "low",
        model_key=key,
        catalog_revision="candidate-probe-v1",
        binding_id=binding.binding_id,
        binding_revision=binding.revision,
        thinking=None if deepseek else "enabled",
        temperature=None,
        top_p=None,
    )


async def probe(key: str, purpose: str, settings: Settings) -> dict[str, object]:
    binding, route = probe_route(key, purpose)
    invocation = str(uuid4())
    result: dict[str, object] = {
        "model_key": key,
        "purpose": purpose,
        "invocation_id": invocation,
        "route_fingerprint": route.fingerprint(),
        "adapter_contract": route.adapter_contract,
        "evidence_kind": "synthetic_connectivity_only",
        "quality_approved": False,
    }
    settings = settings.model_copy(update={"models": ModelConfiguration(bindings=(binding,))})
    schema = {
        "title": "ConnectivityProbe",
        "type": "object",
        "properties": {"ok": {"type": "boolean", "const": True}},
        "required": ["ok"],
        "additionalProperties": False,
    }
    messages = PromptMessages(
        "This is a synthetic connectivity test. Return JSON only.",
        'Return exactly {"ok":true}.',
        "Synthetic input:",
        '{"probe":true}',
    )
    try:
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as client:
            response = await ModelGatewayRouter(client, settings).generate_messages(
                messages,
                route,
                schema,
                invocation,
            )
        validate(json.loads(response.validation_output), schema)
        result.update(
            status="succeeded",
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            latency_ms=response.latency_milliseconds,
        )
    except ProviderFailure as error:
        result.update(status="failed", failure_code=error.code, result_unknown=error.result_unknown)
    except Exception:
        # Upstream bodies, endpoint and credentials never enter probe output.
        result.update(status="failed", failure_code="probe_validation_failed")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-key", required=True, choices=TARGETS)
    parser.add_argument("--purpose", choices=("generation", "semantic"), required=True)
    parser.add_argument("--execute", action="store_true", help="One paid call; no automatic retry")
    args = parser.parse_args()
    _, route = probe_route(args.model_key, args.purpose)
    if not args.execute:
        print(
            json.dumps(
                {
                    "model_key": args.model_key,
                    "purpose": args.purpose,
                    "max_calls": 1,
                    "route_fingerprint": route.fingerprint(),
                    "execute": False,
                }
            )
        )
        return
    result = asyncio.run(probe(args.model_key, args.purpose, Settings()))
    print(json.dumps(result))
    raise SystemExit(0 if result["status"] == "succeeded" else 1)


if __name__ == "__main__":
    main()
