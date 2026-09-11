from qs_ai.application.interpretation.ports import (
    Claim,
    DependencyUnavailable,
    Unauthenticated,
    WorkflowResult,
)
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, EvidenceSet


class UnconfiguredIdentity:
    async def authenticate(self, authorization: str | None) -> Actor:
        if not authorization:
            raise Unauthenticated
        raise DependencyUnavailable("Identity integration is not configured")


class UnconfiguredEvidenceSource:
    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None:
        raise DependencyUnavailable("Authorized evidence integration is not configured")

    async def read(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> tuple[EvidenceItem, ...]:
        raise DependencyUnavailable("Authorized evidence integration is not configured")


class UnconfiguredWorkflow:
    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        raise DependencyUnavailable("Business workflow is not configured")
