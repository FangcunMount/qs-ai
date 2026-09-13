"""Resolve a reviewable evaluation plan without creating or scheduling a Run."""

from dataclasses import dataclass
from typing import Protocol

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef


@dataclass(frozen=True)
class EvaluationPlanQuery:
    scope: DraftScope
    suite: FrozenContractRef
    generation_route: FrozenContractRef
    semantic_route: FrozenContractRef

    def __post_init__(self) -> None:
        if not isinstance(self.scope, DraftScope) or any(
            not isinstance(value, FrozenContractRef)
            for value in (self.suite, self.generation_route, self.semantic_route)
        ):
            raise ValueError("Explicit scope and immutable evaluation references required")


@dataclass(frozen=True)
class EvaluationPlan:
    release: EvidenceReleaseIdentity
    release_fingerprint: str
    generation_case_count: int
    candidates_per_case: int
    candidate_count: int
    preflight_case_count: int
    max_generation_invocations: int
    max_semantic_invocations: int
    execution_policy_json: str
    gate_policy_json: str


class EvaluationPlanner(Protocol):
    async def prepare(self, query: EvaluationPlanQuery) -> EvaluationPlan: ...
