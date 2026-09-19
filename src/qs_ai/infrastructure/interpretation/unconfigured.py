from qs_ai.application.interpretation.ports import (
    Claim,
    DependencyUnavailable,
    WorkflowResult,
)
from qs_ai.domain.interpretation.model import Actor, EvidenceSet


class UnconfiguredEvidenceSource:
    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None:
        raise DependencyUnavailable("Authorized evidence integration is not configured")


class UnconfiguredWorkflow:
    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        raise DependencyUnavailable("Business workflow is not configured")
