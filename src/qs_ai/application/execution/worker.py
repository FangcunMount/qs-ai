import asyncio
from dataclasses import asdict
from uuid import uuid4

from qs_ai.application.interpretation.ports import (
    AccessDenied,
    Claim,
    DependencyUnavailable,
    EvidenceSource,
    ExecutionStore,
    Workflow,
    WorkflowResult,
)
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.interpretation.model import EvidenceSet, RuleViolation


class ExecuteNext:
    def __init__(self, store: ExecutionStore, source: EvidenceSource, workflow: Workflow) -> None:
        self.store = store
        self.source = source
        self.workflow = workflow

    async def _execute(self, claim: Claim) -> None:
        session = claim.session
        try:
            await self.source.authorize(session.actor, session.testee_id, session.assessment_ids)
            evidence = await self.store.evidence(claim)
            if evidence is None and session.workflow_version == "qs-snapshot-v1":
                raise RuleViolation("evidence_missing")
            if evidence is None:
                items = await self.source.read(
                    session.actor, session.testee_id, session.assessment_ids
                )
                evidence = EvidenceSet(
                    str(uuid4()), session.id, fingerprint([asdict(item) for item in items]), items
                )
                evidence.validate(session.testee_id, session.assessment_ids)
                evidence = await self.store.freeze(claim, evidence)
            result = await self.workflow.execute(claim, evidence)
            # Frozen facts do not freeze access rights; recheck before accepting any result.
            await self.source.authorize(session.actor, session.testee_id, session.assessment_ids)
        except AccessDenied:
            result = WorkflowResult("", failure_code="access_revoked")
        except DependencyUnavailable:
            result = WorkflowResult("", failure_code="dependency_unavailable")
        except RuleViolation:
            result = WorkflowResult("", failure_code="evidence_invalid")
        await self.store.finish(claim, result)

    async def once(self, ttl_seconds: int = 30) -> bool:
        if ttl_seconds < 3:
            raise ValueError("Lease TTL must be at least three seconds")
        claim = await self.store.claim(ttl_seconds)
        if claim is None:
            return False

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(ttl_seconds / 3)
                await self.store.renew(claim, ttl_seconds)

        work = asyncio.create_task(self._execute(claim))
        pulse = asyncio.create_task(heartbeat())
        try:
            done, _ = await asyncio.wait({work, pulse}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                work.result()
            else:
                pulse.result()
        finally:
            for task in (work, pulse):
                if not task.done():
                    task.cancel()
            await asyncio.gather(work, pulse, return_exceptions=True)
        return True
