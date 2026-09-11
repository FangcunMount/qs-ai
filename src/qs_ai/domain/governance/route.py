"""Immutable imported definitions; importing is not approval or activation."""

import hashlib
import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RouteAsset:
    route: str
    revision: str
    fingerprint: str
    definition_json: str

    def __post_init__(self) -> None:
        if not self.route.strip() or len(self.route) > 255:
            raise ValueError("Invalid Route identity")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.revision):
            raise ValueError("Invalid Route revision")
        if len(self.definition_json.encode()) > 131072:
            raise ValueError("Route definition exceeds limit")
        definition = json.loads(self.definition_json)
        if not isinstance(definition, dict) or (
            definition.get("route"),
            definition.get("revision"),
        ) != (self.route, self.revision):
            raise ValueError("Route definition identity mismatch")
        required = {
            "route",
            "revision",
            "provider",
            "model",
            "structured_output",
            "idempotent_redispatch",
            "retrieve_by_invocation_id",
            "timeout_milliseconds",
            "max_output_tokens",
        }
        optional = {"reasoning_effort", "protocol", "structured_output_mode"}
        if not required <= definition.keys() or definition.keys() - required - optional:
            raise ValueError("Unexpected model route fields; endpoints and secrets are external")
        for name in ("provider", "model"):
            if not isinstance(definition[name], str) or not definition[name].strip():
                raise ValueError("Invalid model route field")
        for name in ("structured_output", "idempotent_redispatch", "retrieve_by_invocation_id"):
            if type(definition[name]) is not bool:
                raise ValueError("Invalid model route capability")
        for name in ("timeout_milliseconds", "max_output_tokens"):
            if type(definition[name]) is not int or definition[name] <= 0:
                raise ValueError("Invalid model route limit")
        if (
            self.fingerprint
            != "sha256:" + hashlib.sha256(self.definition_json.encode()).hexdigest()
        ):
            raise ValueError("Route fingerprint mismatch")
