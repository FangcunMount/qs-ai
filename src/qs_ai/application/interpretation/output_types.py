"""Shared output contracts; neither scene validator depends on the other."""

from dataclasses import dataclass
from typing import Any, Protocol


class InvalidOutput(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class OutputParser(Protocol):
    def parse(self, raw: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class DeterministicOutput:
    content_json: str
    validator_version: str = "qs-ai-output-deterministic/v1"
