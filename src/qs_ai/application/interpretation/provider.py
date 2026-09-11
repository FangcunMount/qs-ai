import hashlib
import json
from dataclasses import dataclass


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

    def fingerprint(self) -> str:
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
        return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
