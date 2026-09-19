import hashlib
import json
from dataclasses import asdict
from uuid import UUID, uuid4

from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.application.interpretation.ports import (
    AccessDenied,
    AdmissionRejected,
    EvidenceSource,
    Receipt,
    UnitOfWork,
    UnitOfWorkFactory,
)
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceItem,
    EvidenceSet,
    RuleViolation,
    Session,
)


def fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def external_id(value: str) -> bool:
    return (
        len(value) <= 20
        and value.isascii()
        and value.isdecimal()
        and str(int(value)) == value
        and 0 < int(value) < 2**64
    )


def validate_session_input(testee_id: str, assessment_ids: tuple[str, ...], goal: str) -> None:
    if (
        not external_id(testee_id)
        or not 1 <= len(assessment_ids) <= 10
        or len(set(assessment_ids)) != len(assessment_ids)
        or any(not external_id(item) for item in assessment_ids)
        or not goal.strip()
        or len(goal) > 2000
    ):
        raise RuleViolation("invalid_session_input")


class InterpretationService:
    def __init__(self, uows: UnitOfWorkFactory, source: EvidenceSource) -> None:
        self.uows = uows
        self.source = source

    async def _owned(self, actor: Actor, session_id: str) -> Session:
        async with self.uows.open() as uow:
            session = await uow.get(session_id)
        if session.actor != actor:
            raise AccessDenied
        if not session.uses_qs_snapshot:
            await self.source.authorize(actor, session.testee_id, session.assessment_ids)
        return session

    @staticmethod
    def _key(actor: Actor, action: str, resource: str, key: str) -> str:
        if not key or len(key) > 128 or not key.isascii():
            raise RuleViolation("invalid_idempotency_key")
        return fingerprint([asdict(actor), action, resource])

    async def answer(
        self, actor: Actor, session_id: str, command: AnswerCommand, key: str
    ) -> Receipt:
        return await self._change(actor, session_id, command, key)

    async def cancel(
        self, actor: Actor, session_id: str, command: CancelCommand, key: str
    ) -> Receipt:
        return await self._change(actor, session_id, command, key)

    async def _change(
        self,
        actor: Actor,
        session_id: str,
        command: AnswerCommand | CancelCommand,
        key: str,
    ) -> Receipt:
        scope = self._key(actor, command.action, session_id, key)
        await self._owned(actor, session_id)
        async with self.uows.open() as uow:
            previous = await uow.reserve(scope, key, fingerprint(asdict(command)))
            if previous:
                return previous
            session = await uow.get(session_id, lock=True)
            version = command.expected_version
            if not isinstance(version, int) or isinstance(version, bool):
                raise RuleViolation("invalid_version")
            session.expect(version)
            if isinstance(command, CancelCommand):
                session.cancel()
                await uow.cancel_jobs(session)
            else:
                await self._record_answer(uow, session, actor, command)
                session.queue(str(uuid4()), question_id=command.question_id)
                await uow.enqueue(session, command.answer, command.skip, command.question_id)
            await uow.save(session)
            receipt = Receipt(session.id, session.active_run_id, session.status, session.version)
            await uow.receipt(scope, key, receipt)
            await uow.commit()
            return receipt

    async def _record_answer(
        self, uow: UnitOfWork, session: Session, actor: Actor, command: AnswerCommand
    ) -> None:
        if (
            not isinstance(command.question_id, str)
            or not isinstance(command.skip, bool)
            or (command.skip and command.answer is not None)
            or (
                not command.skip
                and (
                    not isinstance(command.answer, str)
                    or not command.answer.strip()
                    or len(command.answer) > 4000
                )
            )
        ):
            raise RuleViolation("invalid_answer")
        # Check ownership of the question before reading or changing it.
        if session.current_question_id != command.question_id:
            raise RuleViolation("question_conflict")
        question = await uow.question(command.question_id)
        if command.skip and not question.can_skip:
            raise RuleViolation("skip_not_allowed")
        if question.answered_by is not None:
            raise RuleViolation("question_conflict")
        await uow.answer(question, actor, command.answer, command.skip)

    async def start_external(
        self,
        actor: Actor,
        testee_id: str,
        assessment_ids: tuple[str, ...],
        goal: str,
        request_id: str,
        evidence: tuple[EvidenceItem, ...] = (),
    ) -> Receipt:
        # Global request scope: reusing an ID for another actor also conflicts.
        if str(UUID(request_id)) != request_id:
            raise RuleViolation("invalid_request_id")
        if not external_id(actor.org_id) or not actor.subject_id or len(actor.subject_id) > 128:
            raise RuleViolation("invalid_session_input")
        validate_session_input(testee_id, assessment_ids, goal)
        scope = fingerprint(["qs-server", "external-start-v1"])
        request_hash = fingerprint(
            [asdict(actor), testee_id, assessment_ids, goal] + [asdict(item) for item in evidence]
        )
        async with self.uows.open() as uow:
            previous = await uow.reserve(scope, request_id, request_hash)
            if previous:
                return previous
            session = Session(str(uuid4()), actor, testee_id, assessment_ids, goal)
            session.queue(str(uuid4()))
            await uow.add(session)
            session.workflow_version = "qs-published-snapshot-v1"
            await uow.bind_request(session.id, request_id)
            try:
                try:
                    EvidenceSet("", "", "", evidence).validate(testee_id, assessment_ids)
                except RuleViolation:
                    raise AdmissionRejected("admission_input_invalid") from None
                frozen = EvidenceSet(
                    str(uuid4()),
                    session.id,
                    fingerprint([asdict(item) for item in evidence]),
                    evidence,
                )
                await uow.add_evidence(frozen)
                session.evidence_set_id = frozen.id
                await uow.bind_configuration(session, frozen)
                try:
                    await uow.enqueue(session, None, False, None)
                except RuleViolation as error:
                    if error.code != "participant_daily_capacity_exceeded":
                        raise
                    raise AdmissionRejected(error.code) from None
            except AdmissionRejected as error:
                # A durable refusal is replayable even after configuration changes.
                # No job/model call is created; the normal outbox informs QS.
                session.running()
                session.block(error.code)
                await uow.reject_run(session)
            await uow.save(session)
            receipt = Receipt(session.id, session.active_run_id, session.status, session.version)
            await uow.receipt(scope, request_id, receipt)
            await uow.commit()
            return receipt
