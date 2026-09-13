"""Version-bound metadata for explicit resolution of uncertain provider calls."""

from dataclasses import dataclass


def validate_unknown_query(expected_version: int) -> None:
    if type(expected_version) is not int or not 1 <= expected_version <= 2**63 - 1:
        raise ValueError("Explicit Run version required")


@dataclass(frozen=True)
class UnknownExecutionSummary:
    execution_id: str
    invocation_id: str
    kind: str
    case_id: str
    slot_ordinal: int
    candidate_id: str
    execution_ordinal: int
    started_at: str
    finished_at: str
    provider_call_count: int
    failure_stage: str
    failure_code: str
    target_execution_count: int
    target_execution_limit: int
    stage_execution_count: int
    stage_execution_limit: int
    # Snapshot eligibility only. Resolve and dispatch reservation recheck independently.
    replacement_allowed: bool


@dataclass(frozen=True)
class UnknownExecutionIndex:
    run_id: str
    version: int
    release_fingerprint: str
    status: str
    unresolved_result_unknown_count: int
    can_resolve: bool
    executions: tuple[UnknownExecutionSummary, ...]
