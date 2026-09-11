"""Synthetic facts and deterministic graph for tests only; never production bindings."""

from langgraph.types import Command

from qs_ai.application.interpretation.ports import AccessDenied, Claim, WorkflowResult
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, EvidenceSet, Fact
from qs_ai.infrastructure.persistence.mysql.leases import Lease
from qs_ai.infrastructure.workflows.langgraph.checkpoints import guarded_saver
from tests.probes.clarification import build_demo


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
    def __init__(self, dsn: str):
        self.dsn = dsn

    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        assert claim.session.workflow_version == "interpretation-v1"
        config = {"configurable": {"thread_id": claim.session.id}}
        async with guarded_saver(self.dsn, Lease(claim.session.id, claim.fence)) as saver:
            graph = build_demo(saver)
            snapshot = await graph.aget_state(config)
            if not snapshot.values:
                await graph.ainvoke({"question": "Who answered?"}, config)
            elif claim.question_id and snapshot.next:
                await graph.ainvoke(
                    Command(resume="[skipped]" if claim.skipped else claim.answer), config
                )
            snapshot = await graph.aget_state(config)
            reference = snapshot.config["configurable"]["checkpoint_id"]
            if snapshot.next:
                return WorkflowResult(reference, question=snapshot.values["question"])
            return WorkflowResult(reference, failure_code="model_not_connected")
