import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol

from qs_ai.application.interpretation.prompts import PromptMessages
from qs_ai.domain.interpretation.model_identity import ModelExecutionIdentity


class ProviderFailure(Exception):
    def __init__(self, code: str, *, retryable: bool = False, result_unknown: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        self.result_unknown = result_unknown
        super().__init__(code)


@dataclass(frozen=True)
class ModelResponse:
    invocation_id: str
    request_id: str
    model: str
    raw_output: str
    validation_output: str
    normalization: str
    input_tokens: int | None
    output_tokens: int | None
    latency_milliseconds: int
    execution_identity: ModelExecutionIdentity | None = None


@dataclass(frozen=True)
class ModelCall:
    invocation_id: str
    status: str
    request_json: str
    response_json: str | None
    failure_code: str | None


@dataclass(frozen=True)
class ModelRoute:
    route: str
    revision: str
    provider: str
    model: str
    protocol: str
    structured_output_mode: str
    timeout_milliseconds: int
    max_output_tokens: int
    reasoning_effort: str
    idempotent_redispatch: bool = False
    retrieve_by_invocation_id: bool = False

    def definition_json(self) -> str:
        # Preserve the original QS route fingerprint field order and omission
        # rules. Credentials and endpoints never belong in this document.
        value: dict[str, object] = {
            "route": self.route,
            "revision": self.revision,
            "provider": self.provider,
            "model": self.model,
            "structured_output": True,
            "idempotent_redispatch": self.idempotent_redispatch,
            "retrieve_by_invocation_id": self.retrieve_by_invocation_id,
            "timeout_milliseconds": self.timeout_milliseconds,
            "max_output_tokens": self.max_output_tokens,
        }
        if self.reasoning_effort:
            value["reasoning_effort"] = self.reasoning_effort
        if self.protocol != "responses":
            value["protocol"] = self.protocol
        if self.structured_output_mode != "json_schema":
            value["structured_output_mode"] = self.structured_output_mode
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        for character, escaped in (
            ("&", "\\u0026"),
            ("<", "\\u003c"),
            (">", "\\u003e"),
            ("\u2028", "\\u2028"),
            ("\u2029", "\\u2029"),
        ):
            raw = raw.replace(character, escaped)
        return raw

    def fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(self.definition_json().encode()).hexdigest()


class MessagesGateway(Protocol):
    async def generate_messages(
        self,
        messages: PromptMessages,
        route: ModelRoute,
        schema: dict[str, Any],
        invocation_id: str,
    ) -> ModelResponse: ...
