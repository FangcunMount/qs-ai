from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol

from qs_ai.domain.interpretation.artifact import ArtifactCandidate
from qs_ai.domain.interpretation.model import Actor, EvidenceSet, Question, Session


class AccessDenied(Exception):
    pass


class DependencyUnavailable(Exception):
    pass


class AdmissionRejected(Exception):
    """Known pre-dispatch refusal; persist it with the original request ID."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class NotFound(Exception):
    pass


class EvidenceSource(Protocol):
    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None: ...


@dataclass(frozen=True)
class Receipt:
    session_id: str
    run_id: str | None
    status: str
    version: int


@dataclass(frozen=True)
class SessionView:
    session: Session
    question: Question | None


class UnitOfWork(Protocol):
    async def reserve(self, scope: str, key: str, request_hash: str) -> Receipt | None: ...
    async def receipt(self, scope: str, key: str, receipt: Receipt) -> None: ...
    async def get(self, session_id: str, *, lock: bool = False) -> Session: ...
    async def bind_request(self, session_id: str, request_id: str) -> None: ...
    async def bind_configuration(self, session: Session, evidence: EvidenceSet) -> None: ...
    async def add(self, session: Session) -> None: ...
    async def add_evidence(self, evidence: EvidenceSet) -> None: ...
    async def save(self, session: Session) -> None: ...
    async def question(self, question_id: str) -> Question: ...
    async def answer(
        self, question: Question, actor: Actor, answer: str | None, skip: bool
    ) -> None: ...
    async def enqueue(
        self, session: Session, answer: str | None, skip: bool, question_id: str | None
    ) -> None: ...
    async def reject_run(self, session: Session) -> None: ...
    async def cancel_jobs(self, session: Session) -> None: ...
    async def commit(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    def open(self) -> AbstractAsyncContextManager[UnitOfWork]: ...


@dataclass(frozen=True)
class Claim:
    job_id: str
    run_id: str
    session: Session
    fence: int
    answer: str | None
    skipped: bool
    question_id: str | None


@dataclass(frozen=True)
class WorkflowResult:
    checkpoint_ref: str
    question: str | None = None
    can_skip: bool = True
    failure_code: str | None = None
    artifact: ArtifactCandidate | None = None


class Workflow(Protocol):
    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult: ...


class ExecutionStore(Protocol):
    async def claim(self, ttl_seconds: int) -> Claim | None: ...
    async def renew(self, claim: Claim, ttl_seconds: int) -> None: ...
    async def evidence(self, claim: Claim) -> EvidenceSet | None: ...
    async def finish(self, claim: Claim, result: WorkflowResult) -> None: ...
