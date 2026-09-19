"""A retry atomically creates a new run, budget reservation, audit and result event."""

from dataclasses import asdict
from uuid import uuid4

from sqlalchemy import insert, select

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.execution.retry import (
    ParticipantExecution,
    ParticipantRetry,
    ParticipantTarget,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import NotFound, Receipt
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork, session_from
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_configurations,
    external_requests,
    jobs,
    model_calls,
    participant_retries,
    runs,
    sessions,
)


class MySQLParticipantRetries:
    def __init__(self, transactions: Transactions, capacity: ParticipantCapacityPolicy) -> None:
        self.transactions, self.capacity = transactions, capacity

    async def get(self, scope: DraftScope, session_id: str) -> ParticipantExecution:
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            row = (
                (
                    await db.execute(
                        select(sessions).where(
                            sessions.c.id == session_id,
                            sessions.c.org_id == scope.organization_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise NotFound
            session = session_from(row)
            request_id = await db.scalar(
                select(external_requests.c.request_id).where(
                    external_requests.c.session_id == session.id,
                )
            )
            if not session.uses_qs_snapshot or request_id is None or session.active_run_id is None:
                raise NotFound
            run_status = await db.scalar(
                select(runs.c.status).where(
                    runs.c.id == session.active_run_id,
                    runs.c.session_id == session.id,
                )
            )
            job_status = await db.scalar(
                select(jobs.c.status).where(
                    jobs.c.run_id == session.active_run_id,
                )
            )
            call = (
                (
                    await db.execute(
                        select(model_calls.c.status, model_calls.c.invocation_id).where(
                            model_calls.c.run_id == session.active_run_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            source_run_id = await db.scalar(
                select(participant_retries.c.source_run_id).where(
                    participant_retries.c.run_id == session.active_run_id,
                    participant_retries.c.session_id == session.id,
                )
            )
            has_configuration = (
                await db.scalar(
                    select(execution_configurations.c.session_id).where(
                        execution_configurations.c.session_id == session.id
                    )
                )
                is not None
            )
            return ParticipantExecution(
                scope.organization_id,
                session.id,
                request_id,
                session.active_run_id,
                session.version,
                session.status,
                session.actor.subject_id,
                session.testee_id,
                session.assessment_ids,
                session.failure_code or "",
                call["status"] if call else "",
                call["invocation_id"] if call else "",
                source_run_id or "",
                session.status == "blocked"
                and run_status == "blocked"
                and job_status in {None, "done", "dead"}
                and has_configuration,
                bool(call and call["status"] in {"dispatched", "unknown"}),
            )

    async def target(self, scope: DraftScope, session_id: str) -> ParticipantTarget:
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(sessions).where(
                            sessions.c.id == session_id, sessions.c.org_id == scope.organization_id
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise NotFound
            session = session_from(row)
            if not session.uses_qs_snapshot:
                raise RuleViolation("participant_retry_requires_snapshot")
            return ParticipantTarget(session.actor, session.testee_id, session.assessment_ids)

    async def receipt(self, scope: DraftScope, command_id: str) -> Receipt:
        async with self.transactions.open() as db:
            raw = await db.scalar(
                select(participant_retries.c.receipt).where(
                    participant_retries.c.organization_id == scope.organization_id,
                    participant_retries.c.command_id == command_id,
                    participant_retries.c.operator_user_id == scope.operator_user_id,
                )
            )
            if raw is None:
                raise NotFound
            return Receipt(**raw)

    async def retry(self, command: ParticipantRetry) -> Receipt:
        async with self.transactions.open() as db:
            uow = MySQLUnitOfWork(db, self.capacity)
            previous = await uow.reserve(
                fingerprint(["participant-retry-v1", command.scope.organization_id]),
                command.command_id,
                fingerprint(asdict(command)),
            )
            if previous is not None:
                return previous
            session = await uow.get(command.session_id, lock=True)
            if int(session.actor.org_id) != command.scope.organization_id:
                raise NotFound
            session.expect(command.expected_version)
            source_run = (
                (
                    await db.execute(
                        select(runs)
                        .where(
                            runs.c.id == command.expected_run_id,
                            runs.c.session_id == session.id,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .one_or_none()
            )
            if source_run is None or source_run["status"] != "blocked":
                raise RuleViolation("participant_retry_requires_blocked_run")
            request_id = await db.scalar(
                select(external_requests.c.request_id).where(
                    external_requests.c.session_id == session.id
                )
            )
            if request_id is None:
                raise RuleViolation("participant_retry_requires_external_request")
            job = (
                (
                    await db.execute(
                        select(jobs)
                        .where(jobs.c.run_id == command.expected_run_id)
                        .with_for_update()
                    )
                )
                .mappings()
                .one_or_none()
            )
            if job is not None and job["status"] not in {"done", "dead"}:
                raise RuleViolation("participant_retry_execution_active")
            call = (
                (
                    await db.execute(
                        select(model_calls)
                        .where(model_calls.c.run_id == command.expected_run_id)
                        .with_for_update()
                    )
                )
                .mappings()
                .one_or_none()
            )
            unknown = call is not None and call["status"] in {"dispatched", "unknown"}
            if unknown and not command.accept_result_unknown_risk:
                raise RuleViolation("participant_retry_unknown_risk_required")
            frozen_request = call["request_json"] if call is not None else None
            if call is None:
                # A retry may itself fail authorization before dispatch. Keep
                # its inherited request when an operator later retries again.
                frozen_request = await db.scalar(
                    select(participant_retries.c.frozen_request_json).where(
                        participant_retries.c.run_id == command.expected_run_id,
                        participant_retries.c.session_id == session.id,
                    )
                )
            if frozen_request is not None:
                try:
                    JSONModelCallCodec().decode_request(frozen_request)
                except ValueError:
                    raise RuleViolation("participant_retry_original_request_invalid") from None
            # Admission refusals have no accepted publication to reuse. Never
            # silently bind a later publication or start an unconfigured retry.
            if (
                await db.scalar(
                    select(execution_configurations.c.session_id).where(
                        execution_configurations.c.session_id == session.id
                    )
                )
                is None
            ):
                raise RuleViolation("participant_retry_requires_accepted_configuration")
            failure = session.failure_code
            session.retry(command.expected_run_id, str(uuid4()))
            await uow.enqueue(session, None, False, None)
            await uow.save(session)
            receipt = Receipt(session.id, session.active_run_id, session.status, session.version)
            await db.execute(
                insert(participant_retries).values(
                    organization_id=command.scope.organization_id,
                    command_id=command.command_id,
                    session_id=session.id,
                    request_id=request_id,
                    source_run_id=command.expected_run_id,
                    run_id=session.active_run_id,
                    operator_user_id=command.scope.operator_user_id,
                    expected_version=command.expected_version,
                    reason=command.reason,
                    accepted_unknown_risk=command.accept_result_unknown_risk,
                    source_failure_code=failure,
                    frozen_request_json=frozen_request,
                    receipt=asdict(receipt),
                )
            )
            await uow.receipt(
                fingerprint(["participant-retry-v1", command.scope.organization_id]),
                command.command_id,
                receipt,
            )
            await uow.commit()
            return receipt
