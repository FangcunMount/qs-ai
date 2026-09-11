"""Frozen policy projections; classification and atomic execution reservation are separate."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionPolicy:
    policy_id: str
    version: str
    fingerprint: str
    definition_json: str
    generation_cases: int
    candidates_per_case: int
    preflight_cases: int
    generation_per_slot: int
    generation_per_run: int
    semantic_per_candidate: int
    semantic_per_run: int
    automatic_selectors: frozenset[tuple[str, str]]
    manual_selectors: frozenset[tuple[str, str]]
    unknown_requires_acknowledgement: bool
    quality_replacement_allowed: bool
    semantic_failure_regenerates_candidate: bool

    def selects_automatic_retry(self, stage: str, code: str, *, retryable: bool) -> bool:
        """Policy whitelist only; callers must also validate failure disposition and budget."""
        return retryable and (stage, code) in self.automatic_selectors

    def within_budget(self, stage: str, target_executions: int, run_executions: int) -> bool:
        """Check before reservation; counts include failed and unknown dispatched calls."""
        if any(type(x) is not int or x < 0 for x in (target_executions, run_executions)):
            raise ValueError("Invalid execution counts")
        if target_executions > run_executions:
            raise ValueError("Target count exceeds run count")
        if stage == "generation_execution":
            return (
                target_executions < self.generation_per_slot
                and run_executions < self.generation_per_run
            )
        if stage == "semantic_evaluation":
            return (
                target_executions < self.semantic_per_candidate
                and run_executions < self.semantic_per_run
            )
        raise ValueError("Unsupported evaluation execution stage")
