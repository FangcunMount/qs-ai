"""Synthetic facts and deterministic workflow for tests only; never production bindings."""

from qs_ai.application.interpretation.ports import AccessDenied, Claim, WorkflowResult
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, EvidenceSet, Fact


class SyntheticEvidence:
    revoked = False
    reads = 0

    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None:
        if self.revoked:
            raise AccessDenied

    async def read(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> tuple[EvidenceItem, ...]:
        await self.authorize(actor, testee_id, assessment_ids)
        self.reads += 1
        return tuple(
            EvidenceItem(
                item,
                testee_id,
                f"report-{item}",
                "fixture-v1",
                (Fact("dimensions.attention", "synthetic fact"),),
            )
            for item in assessment_ids
        )


class OfflineWorkflow:
    """Deterministic adapter: business state owns interruption and recovery in tests."""

    def __init__(self, dsn: str):
        pass

    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        assert claim.session.workflow_version in {
            "interpretation-v1",
            "qs-snapshot-v1",
            "qs-published-snapshot-v1",
        }
        reference = f"offline:{claim.run_id}"
        if claim.question_id:
            return WorkflowResult(reference, failure_code="model_not_connected")
        return WorkflowResult(reference, question="Who answered?")
