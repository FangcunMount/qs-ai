from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol

from qs_ai.domain.interpretation.model import Actor, EvidenceItem, EvidenceSet, Question, Session


class Unauthenticated(Exception):
    pass


class AccessDenied(Exception):
    pass


class DependencyUnavailable(Exception):
    pass


class NotFound(Exception):
    pass


class IdentityVerifier(Protocol):
    async def authenticate(self, authorization: str | None) -> Actor: ...


class EvidenceSource(Protocol):
    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None: ...

    async def read(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> tuple[EvidenceItem, ...]: ...


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
    async def add(self, session: Session) -> None: ...
    async def save(self, session: Session) -> None: ...
    async def question(self, question_id: str) -> Question: ...
    async def answer(
        self, question: Question, actor: Actor, answer: str | None, skip: bool
    ) -> None: ...
    async def enqueue(
        self, session: Session, answer: str | None, skip: bool, question_id: str | None
    ) -> None: ...
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


class Workflow(Protocol):
    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult: ...


class ExecutionStore(Protocol):
    async def claim(self, ttl_seconds: int) -> Claim | None: ...
    async def renew(self, claim: Claim, ttl_seconds: int) -> None: ...
    async def evidence(self, claim: Claim) -> EvidenceSet | None: ...
    async def freeze(self, claim: Claim, evidence: EvidenceSet) -> EvidenceSet: ...
    async def finish(self, claim: Claim, result: WorkflowResult) -> None: ...
