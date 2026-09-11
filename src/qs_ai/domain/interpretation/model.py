from dataclasses import dataclass
from enum import StrEnum


class RuleViolation(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class Status(StrEnum):
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    AWAITING_ANSWER = "awaiting_answer"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class Actor:
    org_id: str
    subject_id: str

    def __post_init__(self) -> None:
        if not self.org_id or not self.subject_id:
            raise RuleViolation("invalid_actor")


@dataclass(frozen=True)
class Fact:
    ref: str
    value: str


@dataclass(frozen=True)
class EvidenceItem:
    assessment_id: str
    testee_id: str
    report_id: str
    source_version: str
    facts: tuple[Fact, ...]


@dataclass(frozen=True)
class EvidenceSet:
    id: str
    session_id: str
    fingerprint: str
    items: tuple[EvidenceItem, ...]

    def validate(self, testee_id: str, assessment_ids: tuple[str, ...]) -> None:
        if (
            not self.items
            or len(self.items) != len(assessment_ids)
            or {item.assessment_id for item in self.items} != set(assessment_ids)
        ):
            raise RuleViolation("evidence_incomplete")
        for item in self.items:
            if item.testee_id != testee_id:
                raise RuleViolation("evidence_subject_mismatch")
            if (
                not item.report_id
                or not item.source_version
                or not item.facts
                or any(not fact.ref for fact in item.facts)
                or len({fact.ref for fact in item.facts}) != len(item.facts)
            ):
                raise RuleViolation("evidence_identity_missing")


@dataclass
class Session:
    id: str
    actor: Actor
    testee_id: str
    assessment_ids: tuple[str, ...]
    goal: str
    status: Status = Status.CREATED
    version: int = 1
    active_run_id: str | None = None
    current_question_id: str | None = None
    evidence_set_id: str | None = None
    workflow_version: str = "interpretation-v1"
    failure_code: str | None = None

    def expect(self, version: int) -> None:
        if self.version != version:
            raise RuleViolation("version_conflict")

    def queue(self, run_id: str, *, question_id: str | None = None) -> None:
        if question_id is None:
            if self.status != Status.CREATED:
                raise RuleViolation("invalid_state")
        elif self.status != Status.AWAITING_ANSWER or self.current_question_id != question_id:
            raise RuleViolation("question_conflict")
        self.active_run_id = run_id
        self.current_question_id = None
        self.status = Status.QUEUED
        self.version += 1

    def running(self) -> None:
        if self.status not in (Status.QUEUED, Status.RUNNING):
            raise RuleViolation("invalid_state")
        self.status = Status.RUNNING
        self.version += 1

    def await_answer(self, question_id: str) -> None:
        if self.status != Status.RUNNING:
            raise RuleViolation("invalid_state")
        self.current_question_id = question_id
        self.status = Status.AWAITING_ANSWER
        self.version += 1

    def block(self, code: str) -> None:
        if self.status != Status.RUNNING:
            raise RuleViolation("invalid_state")
        self.status = Status.BLOCKED
        self.failure_code = code
        self.version += 1

    def cancel(self) -> None:
        if self.status == Status.CANCELLED:
            raise RuleViolation("invalid_state")
        self.status = Status.CANCELLED
        self.current_question_id = None
        self.version += 1


@dataclass(frozen=True)
class Question:
    id: str
    session_id: str
    text: str
    can_skip: bool = True
    answer: str | None = None
    skipped: bool = False
    answered_by: str | None = None
