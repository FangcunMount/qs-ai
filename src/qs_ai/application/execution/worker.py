import asyncio
import logging

from qs_ai.application.interpretation.ports import (
    AccessDenied,
    Claim,
    DependencyUnavailable,
    EvidenceSource,
    ExecutionStore,
    Workflow,
    WorkflowResult,
)
from qs_ai.application.operations.diagnostics import attempt_context, classify, emit, operation
from qs_ai.domain.interpretation.model import RuleViolation


class ExecuteNext:
    def __init__(self, store: ExecutionStore, source: EvidenceSource, workflow: Workflow) -> None:
        self.store = store
        self.source = source
        self.workflow = workflow

    async def _execute(self, claim: Claim) -> None:
        session = claim.session
        try:
            with operation("generation.authorization", "generation"):
                await self.source.authorize(
                    session.actor, session.testee_id, session.assessment_ids
                )
            with operation("generation.frozen_evidence", "generation"):
                evidence = await self.store.evidence(claim)
            if evidence is None:
                raise RuleViolation("evidence_missing")
            with operation("generation.workflow", "generation"):
                result = await self.workflow.execute(claim, evidence)
            # Frozen facts do not freeze access rights; recheck before accepting any result.
            with operation("generation.authorization_recheck", "generation"):
                await self.source.authorize(
                    session.actor, session.testee_id, session.assessment_ids
                )
        except AccessDenied:
            result = WorkflowResult("", failure_code="access_revoked")
        except DependencyUnavailable:
            result = WorkflowResult("", failure_code="dependency_unavailable")
        except RuleViolation:
            result = WorkflowResult("", failure_code="evidence_invalid")
        await self.store.finish(claim, result)
        emit(
            "generation.result_committed",
            "generation",
            status="failed" if result.failure_code else "completed",
            error_code=result.failure_code or "none",
        )

    async def _observed_execute(self, claim: Claim) -> None:
        with operation("generation", "generation"):
            await self._execute(claim)

    async def once(self, ttl_seconds: int = 30) -> bool:
        if ttl_seconds < 3:
            raise ValueError("Lease TTL must be at least three seconds")
        claim = await self.store.claim(ttl_seconds)
        if claim is None:
            return False

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(ttl_seconds / 3)
                try:
                    await self.store.renew(claim, ttl_seconds)
                except Exception as error:
                    emit(
                        "generation.lease_failed",
                        "generation",
                        level=logging.WARNING,
                        error_code=classify(error),
                        error_type=type(error).__name__,
                    )
                    raise

        with attempt_context(session_id=claim.session.id, run_id=claim.run_id):
            work = asyncio.create_task(self._observed_execute(claim))
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
