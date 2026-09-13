"""Read-only execution diagnostics, separate from candidate approval and recovery commands."""

import re
from dataclasses import dataclass
from typing import Protocol

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.evaluation.unknowns import validate_unknown_query


@dataclass(frozen=True)
class ExecutionQuery:
    scope: ManagementScope
    expected_version: int
    cursor: str = ""
    limit: int = 20

    def __post_init__(self) -> None:
        validate_unknown_query(self.expected_version)
        if type(self.limit) is not int or not 1 <= self.limit <= 50:
            raise ValueError("Execution page limit must be between 1 and 50")
        if self.cursor and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", self.cursor):
            raise ValueError("Execution cursor is invalid")


@dataclass(frozen=True)
class ExecutionSummary:
    execution_id: str
    invocation_id: str
    kind: str
    case_id: str
    slot_ordinal: int
    execution_ordinal: int
    status: str
    raw_output_bytes: int
    normalized_output_bytes: int
    evidence_json: str


@dataclass(frozen=True)
class ExecutionPage:
    run_id: str
    version: int
    executions: tuple[ExecutionSummary, ...]
    next_cursor: str


@dataclass(frozen=True)
class ExecutionOutput:
    run_id: str
    version: int
    execution: ExecutionSummary
    raw_output: bytes
    normalized_output: bytes
    raw_sha256: str
    normalized_sha256: str


class EvaluationDiagnostics(Protocol):
    async def list(self, query: ExecutionQuery) -> ExecutionPage: ...
    async def get(self, query: ExecutionQuery, execution_id: str) -> ExecutionOutput: ...
