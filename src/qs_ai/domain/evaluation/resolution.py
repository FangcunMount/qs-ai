"""Explicit result-unknown decisions; original execution evidence remains immutable."""

import re
from dataclasses import dataclass
from datetime import datetime

from qs_ai.domain.evaluation.policy import ExecutionPolicy


def valid_id(value: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", value) is not None


@dataclass(frozen=True)
class ResultUnknownResolution:
    execution_id: str
    decision: str
    actor: str
    reason: str
    acknowledged_duplicate_call_and_cost_risk: bool
    resolved_at: datetime

    def __post_init__(self) -> None:
        if (
            not valid_id(self.execution_id)
            or not valid_id(self.actor)
            or self.decision not in ("authorize_replacement", "cancel_run")
            or self.acknowledged_duplicate_call_and_cost_risk is not True
            or not self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
            or self.resolved_at.tzinfo is None
            or self.resolved_at.utcoffset() is None
        ):
            raise ValueError("Invalid result-unknown resolution audit")


@dataclass(frozen=True)
class UnknownExecution:
    execution_id: str
    kind: str
    execution_ordinal: int
    finished_at: datetime

    def __post_init__(self) -> None:
        if (
            not valid_id(self.execution_id)
            or self.kind not in ("generation", "semantic")
            or type(self.execution_ordinal) is not int
            or self.execution_ordinal < 1
            or self.finished_at.tzinfo is None
            or self.finished_at.utcoffset() is None
        ):
            raise ValueError("Invalid unknown execution evidence")


@dataclass(frozen=True)
class ResolutionProgress:
    status: str
    unresolved_count: int
    resolutions: tuple[ResultUnknownResolution, ...]


def resolve_unknown(
    status: str,
    unknown_executions: tuple[UnknownExecution, ...],
    resolutions: tuple[ResultUnknownResolution, ...],
    value: ResultUnknownResolution,
    policy: ExecutionPolicy,
) -> ResolutionProgress:
    """Caller must supply all persisted unknown executions and prior decisions, under CAS."""
    if status != "blocked":
        raise ValueError("Blocked Run required")
    if not isinstance(unknown_executions, tuple) or not isinstance(resolutions, tuple):
        raise ValueError("Immutable evidence and resolution history required")
    executions = {e.execution_id: e for e in unknown_executions}
    prior = {r.execution_id: r for r in resolutions}
    if len(executions) != len(unknown_executions) or len(prior) != len(resolutions):
        raise ValueError("Duplicate execution or resolution evidence")
    if any(
        key not in executions
        or r.resolved_at < executions[key].finished_at
        or r.decision == "cancel_run"
        for key, r in prior.items()
    ):
        raise ValueError("Prior resolution does not match a blocked Run")
    target = executions.get(value.execution_id)
    if target is None or value.execution_id in prior or value.resolved_at < target.finished_at:
        raise ValueError("Unresolved execution and valid resolution time required")
    limit = (
        policy.generation_per_slot if target.kind == "generation" else policy.semantic_per_candidate
    )
    if value.decision == "authorize_replacement" and target.execution_ordinal >= limit:
        raise ValueError("Frozen recovery budget exhausted")
    remaining = len(executions) - len(prior) - 1
    new_status = (
        "canceled"
        if value.decision == "cancel_run"
        else ("collecting" if remaining == 0 else "blocked")
    )
    return ResolutionProgress(new_status, remaining, (*resolutions, value))
