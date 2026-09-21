"""Bound response evidence and classify contract failures before terminal persistence."""

import json
from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute
from qs_ai.domain.evaluation.completion import ProviderReceipt
from qs_ai.domain.evaluation.failure import ClassifiedFailure


@dataclass(frozen=True)
class ResponseEvidence:
    receipt: ProviderReceipt | None
    raw: bytes
    normalized: bytes
    failure: ClassifiedFailure | None


def _object(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise ValueError("Invalid JSON constant")


def response_evidence(
    stage: str,
    execution_id: str,
    invocation_id: str,
    route: ModelRoute,
    schema: dict,
    response: ModelResponse,
) -> ResponseEvidence:
    if stage not in ("generation", "semantic"):
        raise ValueError("Unsupported response stage")
    raw, normalized = b"", b""
    code = ""
    try:
        raw, normalized = response.raw_output.encode(), response.validation_output.encode()
    except UnicodeError:
        code = "output_schema_invalid"
    if len(raw) > 256 * 1024 or len(normalized) > 256 * 1024:
        raw, normalized = b"", b""
        code = "output_missing_or_too_large"
    elif not raw or not normalized:
        code = code or "output_missing_or_too_large"
    receipt = None
    try:
        if response.invocation_id != invocation_id or response.model != route.model:
            raise ValueError("Receipt mismatch")
        receipt = ProviderReceipt(
            invocation_id,
            response.request_id,
            route.provider,
            response.model,
            response.input_tokens,
            response.output_tokens,
            response.latency_milliseconds * 1_000_000,
        )
    except ValueError:
        code = "receipt_invalid"
    valid_json = False
    if normalized:
        try:
            value = json.loads(normalized, object_pairs_hook=_object, parse_constant=_constant)
            valid_json = True
            if not Draft202012Validator(schema).is_valid(value):
                code = code or "output_schema_invalid"
        except ValueError:
            code = code or "output_schema_invalid"
    # Generation terminal JSON is required even on failure; the original response
    # remains evidence. Semantic failures can also retain invalid normalized bytes.
    if stage == "generation" and not valid_json:
        normalized = b""
    if not code:
        return ResponseEvidence(receipt, raw, normalized, None)
    if stage == "semantic":
        failure = ClassifiedFailure(
            "semantic_evaluation",
            "semantic_execution",
            "semantic_" + code,
            code != "receipt_invalid",
            False,
            "retry_semantic",
            "Semantic response evidence invalid",
            (execution_id,),
        )
    elif code == "receipt_invalid":
        failure = ClassifiedFailure(
            "generation_execution",
            "provider_protocol",
            "provider_receipt_invalid",
            False,
            False,
            "no_action",
            "Provider receipt invalid",
            (execution_id,),
        )
    else:
        failure = ClassifiedFailure(
            "output_validation",
            "output_contract_conformance",
            code,
            False,
            False,
            "replace_generation",
            "Generated output violates contract",
            (execution_id,),
        )
    return ResponseEvidence(receipt, raw, normalized, failure)
