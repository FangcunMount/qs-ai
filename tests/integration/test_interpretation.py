import asyncio
import os
import sys
from dataclasses import asdict, dataclass
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select, text, update

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.interpretation.commands import AnswerCommand, CancelCommand
from qs_ai.application.interpretation.ports import (
    AccessDenied,
    Receipt,
    WorkflowResult,
)
from qs_ai.application.interpretation.service import InterpretationService, fingerprint
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceItem,
    EvidenceSet,
    Fact,
    RuleViolation,
    Session,
    Status,
)
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.execution import MySQLExecutionStore
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.schema import (
    artifacts,
    evidence_sets,
    execution_configurations,
    execution_leases,
    external_requests,
    idempotency,
    jobs,
    model_calls,
    participant_capacity_reservations,
    participant_retries,
    questions,
    result_outbox,
    runs,
    sessions,
)
from tests.probes.p1_runtime import OfflineWorkflow, SyntheticEvidence
from tests.probes.session_inspection import read_session

pytestmark = pytest.mark.integration


@dataclass
class Kit:
    dsn: str
    transactions: Transactions
    service: InterpretationService
    store: MySQLExecutionStore
    source: SyntheticEvidence
    actor: Actor

    async def create(self):
        session = Session(str(uuid4()), self.actor, "18446744073709551615", ("42",), "goal")
        # Low-level persistence tests start with facts already frozen, just like QS admission.
        async with self.service.uows.open() as uow:
            await uow.add(session)
            items = (
                EvidenceItem(
                    "42",
                    session.testee_id,
                    "report-42",
                    "fixture-v1",
                    (Fact("dimensions.attention", "synthetic fact"),),
                ),
            )
            evidence = EvidenceSet(
                str(uuid4()), session.id, fingerprint([asdict(item) for item in items]), items
            )
            evidence.validate(session.testee_id, session.assessment_ids)
            await uow.add_evidence(evidence)
            session.evidence_set_id = evidence.id
            await uow.save(session)
            await uow.commit()
        return Receipt(session.id, None, session.status, session.version)

    async def queued(self):
        receipt = await self.create()
        async with self.service.uows.open() as uow:
            session = await uow.get(receipt.session_id)
            session.queue(str(uuid4()))
            await uow.enqueue(session, None, False, None)
            await uow.save(session)
            await uow.commit()
        return Receipt(session.id, session.active_run_id, session.status, session.version)

    def worker(self):
        return ExecuteNext(self.store, self.source, OfflineWorkflow(self.dsn))


@pytest.fixture
async def kit():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires disposable MySQL with migrations applied")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    source = SyntheticEvidence()
    actor = Actor("1", str(uuid4()))
    service = InterpretationService(MySQLUnitOfWorkFactory(transactions), source)
    try:
        yield Kit(dsn, transactions, service, MySQLExecutionStore(transactions), source, actor)
    finally:
        async with transactions.open() as db:
            ids = list(
                (
                    await db.scalars(
                        select(sessions.c.id).where(sessions.c.owner_subject_id == actor.subject_id)
                    )
                ).all()
            )
            await db.execute(
                delete(model_calls).where(
                    model_calls.c.run_id.in_(select(runs.c.id).where(runs.c.session_id.in_(ids)))
                )
            )
            for table in (
                participant_retries,
                participant_capacity_reservations,
                execution_configurations,
                result_outbox,
                external_requests,
                artifacts,
                jobs,
                runs,
                questions,
                evidence_sets,
            ):
                await db.execute(delete(table).where(table.c.session_id.in_(ids)))
            await db.execute(delete(sessions).where(sessions.c.id.in_(ids)))
            await db.execute(delete(execution_leases).where(execution_leases.c.thread_id.in_(ids)))
            # Receipts only contain session IDs; restrict cleanup to this fixture's IDs.
            for row in (await db.execute(select(idempotency))).mappings().all():
                if row["response"] and row["response"]["session_id"] in ids:
                    await db.execute(
                        delete(idempotency).where(
                            idempotency.c.scope_hash == row["scope_hash"],
                            idempotency.c.key == row["key"],
                        )
                    )
            await db.commit()
        await database.close()


async def expire(kit, session_id):
    async with kit.transactions.open() as db:
        await db.execute(
            update(jobs)
            .where(jobs.c.session_id == session_id)
            .values(lease_until=text("TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))"))
        )
        await db.execute(
            update(execution_leases)
            .where(execution_leases.c.thread_id == session_id)
            .values(expires_at=text("TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))"))
        )
        await db.commit()


async def test_model_dispatch_is_durable_and_only_one_caller_can_send(kit):
    await kit.queued()
    claim = await kit.store.claim(60)
    assert claim is not None
    attempts = await asyncio.gather(
        kit.store.begin_model_call(claim, '{"input":"frozen"}'),
        kit.store.begin_model_call(claim, '{"input":"frozen"}'),
    )
    assert sum(created for _, created in attempts) == 1
    assert attempts[0][0] == attempts[1][0]
    # A fresh store/connection sees the committed marker, not permission to send again.
    recovered, created = await MySQLExecutionStore(kit.transactions).begin_model_call(
        claim, '{"input":"changed"}'
    )
    assert not created
    assert recovered.request_json == '{"input":"frozen"}'
    assert recovered.status == "dispatched"


async def test_model_response_survives_reclaim_without_redispatch(kit):
    receipt = await kit.queued()
    claim = await kit.store.claim(60)
    call, created = await kit.store.begin_model_call(claim, "{}")
    assert created
    await kit.store.record_model_response(
        claim, call.invocation_id, response_json='{"raw_output":"retained"}'
    )
    await expire(kit, receipt.session_id)
    replacement = await kit.store.claim(60)
    assert replacement.fence > claim.fence
    recovered, created = await kit.store.begin_model_call(replacement, "{}")
    assert not created
    assert recovered.status == "response_received"
    assert recovered.response_json == '{"raw_output":"retained"}'
    with pytest.raises(LeaseLost):
        await kit.store.record_model_response(
            claim, call.invocation_id, response_json='{"raw_output":"late"}'
        )


async def test_unacknowledged_dispatch_cannot_be_resent_or_overwritten_after_reclaim(kit):
    receipt = await kit.queued()
    claim = await kit.store.claim(60)
    call, _ = await kit.store.begin_model_call(claim, "{}")
    await expire(kit, receipt.session_id)
    replacement = await kit.store.claim(60)
    recovered, created = await kit.store.begin_model_call(replacement, "{}")
    assert recovered == call
    assert not created
    # Neither a stale worker nor its replacement may fabricate a provider receipt.
    for owner in (claim, replacement):
        with pytest.raises(LeaseLost):
            await kit.store.record_model_response(owner, call.invocation_id, response_json="{}")


@pytest.mark.parametrize("unknown", [False, True])
async def test_model_failure_is_retained_and_receipt_is_immutable(kit, unknown):
    await kit.queued()
    claim = await kit.store.claim(60)
    call, _ = await kit.store.begin_model_call(claim, "{}")
    await kit.store.record_model_response(
        claim, call.invocation_id, failure_code="provider_timeout", result_unknown=unknown
    )
    recovered, created = await kit.store.begin_model_call(claim, "{}")
    assert not created
    assert recovered.status == ("unknown" if unknown else "failed")
    assert recovered.failure_code == "provider_timeout"
    with pytest.raises(LeaseLost):
        await kit.store.record_model_response(claim, call.invocation_id, response_json="{}")


@pytest.fixture
async def published_kit(published_configuration):
    # Execution snapshots reference publications: remove sessions before publication teardown.
    async for value in kit.__wrapped__():
        yield value


async def test_external_start_replay_and_key_conflicts(published_kit):
    from tests.test_input_binding import bound_case

    kit = published_kit
    request_id = str(uuid4())
    evidence = bound_case()[1].items
    a, b = await asyncio.gather(
        *[
            kit.service.start_external(kit.actor, "7", ("42",), "same", request_id, evidence)
            for _ in range(2)
        ]
    )
    assert a == b and a.status == Status.QUEUED
    with pytest.raises(RuleViolation, match="idempotency_conflict"):
        await kit.service.start_external(kit.actor, "7", ("42",), "different", request_id, evidence)
    assert (
        await kit.service.start_external(kit.actor, "7", ("42",), "same", request_id, evidence) == a
    )
    async with kit.transactions.open() as db:
        assert await db.scalar(select(func.count()).select_from(jobs)) == 1


@pytest.mark.parametrize("skip", [False, True])
async def test_answer_resume_and_immutable_evidence(kit, skip):
    receipt = await kit.queued()
    assert await kit.worker().once()
    view = await read_session(kit.service.uows, receipt.session_id)
    assert view.session.status == Status.AWAITING_ANSWER
    question = view.question
    payload = {
        "expected_version": view.session.version,
        "question_id": question.id,
        "answer": None if skip else "Father",
        "skip": skip,
    }
    result = await kit.service.answer(
        kit.actor, receipt.session_id, AnswerCommand(**payload), "answer"
    )
    assert (
        await kit.service.answer(kit.actor, receipt.session_id, AnswerCommand(**payload), "answer")
        == result
    )
    assert await kit.worker().once()
    done = await read_session(kit.service.uows, receipt.session_id)
    assert done.session.status == Status.BLOCKED
    assert done.session.failure_code == "model_not_connected"
    async with kit.transactions.open() as db:
        row = (
            (await db.execute(select(questions).where(questions.c.id == question.id)))
            .mappings()
            .one()
        )
        assert row["skipped"] == skip
        assert row["answered_by"] == kit.actor.subject_id
        count = await db.scalar(
            select(func.count())
            .select_from(evidence_sets)
            .where(evidence_sets.c.session_id == receipt.session_id)
        )
        assert count == 1


async def test_concurrent_answers_accept_once(kit):
    receipt = await kit.queued()
    await kit.worker().once()
    view = await read_session(kit.service.uows, receipt.session_id)
    payload = {
        "expected_version": view.session.version,
        "question_id": view.question.id,
        "answer": "Father",
        "skip": False,
    }
    results = await asyncio.gather(
        *[
            kit.service.answer(kit.actor, receipt.session_id, AnswerCommand(**payload), key)
            for key in ("one", "two")
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(result, RuleViolation) for result in results) == 1
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(jobs)
                .where(jobs.c.session_id == receipt.session_id)
            )
            == 2
        )


async def test_wrong_question_and_stale_version_leave_no_partial_work(kit):
    receipt = await kit.queued()
    await kit.worker().once()
    view = await read_session(kit.service.uows, receipt.session_id)
    for version, question_id, code in [
        (1, view.question.id, "version_conflict"),
        (view.session.version, str(uuid4()), "question_conflict"),
    ]:
        with pytest.raises(RuleViolation, match=code):
            await kit.service.answer(
                kit.actor,
                receipt.session_id,
                AnswerCommand(expected_version=version, question_id=question_id, answer="Father"),
                code,
            )
    unchanged = await read_session(kit.service.uows, receipt.session_id)
    assert unchanged == view


async def test_owner_isolation_and_revocation_even_on_replay(kit):
    created = await kit.queued()
    command = CancelCommand(created.version)
    for actor in (Actor("1", "another-parent"), Actor("2", kit.actor.subject_id)):
        with pytest.raises(AccessDenied):
            await kit.service.cancel(actor, created.session_id, command, "cancel")
    receipt = await kit.service.cancel(kit.actor, created.session_id, command, "cancel")
    assert await kit.service.cancel(kit.actor, created.session_id, command, "cancel") == receipt
    pending = await kit.queued()
    kit.source.revoked = True
    with pytest.raises(AccessDenied):
        await kit.service.cancel(kit.actor, created.session_id, command, "cancel")
    assert await kit.worker().once()
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(sessions.c.failure_code).where(sessions.c.id == pending.session_id)
            )
            == "access_revoked"
        )


async def test_takeover_cancel_and_stale_business_publication(kit):
    receipt = await kit.queued()
    first = await kit.store.claim(30)
    assert first is not None
    assert await kit.store.claim(30) is None
    await expire(kit, receipt.session_id)
    results = await asyncio.gather(kit.store.claim(30), kit.store.claim(30))
    second = next(result for result in results if result is not None)
    assert sum(result is not None for result in results) == 1
    assert second.fence > first.fence
    with pytest.raises(LeaseLost):
        await kit.store.finish(first, WorkflowResult("cp", question="old"))
    with pytest.raises(LeaseLost):
        await kit.store.renew(first, 30)
    await kit.service.cancel(
        kit.actor,
        receipt.session_id,
        CancelCommand(expected_version=second.session.version),
        "cancel",
    )
    with pytest.raises(LeaseLost):
        await kit.store.finish(second, WorkflowResult("cp", question="late"))
    assert (
        await read_session(kit.service.uows, receipt.session_id)
    ).session.status == Status.CANCELLED


async def test_restart_between_workflow_return_and_business_commit(kit):
    receipt = await kit.queued()
    args = [sys.executable, "-m", "tests.probes.p1_process"]
    first = await asyncio.create_subprocess_exec(
        *args, "crash-window", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        assert await asyncio.wait_for(first.stdout.readline(), 20) == b"WORKFLOW_RETURNED\n"
    finally:
        if first.returncode is None:
            first.kill()
        await first.communicate()
    view = await read_session(kit.service.uows, receipt.session_id)
    assert view.session.status == Status.RUNNING
    assert view.question is None
    await expire(kit, receipt.session_id)
    second = await asyncio.create_subprocess_exec(
        *args, "recover", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        _, error = await asyncio.wait_for(second.communicate(), 20)
        assert second.returncode == 0, error.decode()
    finally:
        if second.returncode is None:
            second.kill()
            await second.communicate()
    view = await read_session(kit.service.uows, receipt.session_id)
    assert view.session.status == Status.AWAITING_ANSWER
    assert view.question.text == "Who answered?"


async def test_heartbeat_keeps_live_attempt_owned(kit):
    receipt = await kit.queued()
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowWorkflow(OfflineWorkflow):
        async def execute(self, claim, evidence):
            entered.set()
            await release.wait()
            return await super().execute(claim, evidence)

    work = asyncio.create_task(ExecuteNext(kit.store, kit.source, SlowWorkflow(kit.dsn)).once(3))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await asyncio.sleep(3.2)  # Exceed original lease; heartbeat must extend it.
        assert await kit.store.claim(3) is None
        release.set()
        assert await asyncio.wait_for(work, 5)
    finally:
        if not work.done():
            work.cancel()
        await asyncio.gather(work, return_exceptions=True)
    assert (
        await read_session(kit.service.uows, receipt.session_id)
    ).session.status == Status.AWAITING_ANSWER


async def test_lost_lease_cancels_work_and_prevents_publication(kit):
    receipt = await kit.queued()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    class WaitingWorkflow:
        async def execute(self, claim, evidence):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    worker = asyncio.create_task(ExecuteNext(kit.store, kit.source, WaitingWorkflow()).once(3))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await expire(kit, receipt.session_id)
        with pytest.raises(LeaseLost):
            await asyncio.wait_for(worker, 5)
        assert cancelled.is_set()
        view = await read_session(kit.service.uows, receipt.session_id)
        assert view.question is None
    finally:
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_crash_attempt_budget_blocks_infinite_reclaim(kit):
    receipt = await kit.queued()
    for _ in range(3):
        assert await kit.store.claim(3) is not None
        await expire(kit, receipt.session_id)
    assert await kit.store.claim(3) is None
    view = await read_session(kit.service.uows, receipt.session_id)
    assert view.session.failure_code == "attempts_exhausted"


async def test_revocation_during_work_never_publishes_question(kit):
    receipt = await kit.queued()

    class RevokingWorkflow(OfflineWorkflow):
        async def execute(self, claim, evidence):
            result = await super().execute(claim, evidence)
            kit.source.revoked = True
            return result

    await ExecuteNext(kit.store, kit.source, RevokingWorkflow(kit.dsn)).once()
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(questions)
                .where(questions.c.session_id == receipt.session_id)
            )
            == 0
        )
        assert (
            await db.scalar(
                select(sessions.c.failure_code).where(sessions.c.id == receipt.session_id)
            )
            == "access_revoked"
        )


async def test_failure_after_job_insert_rolls_back_command_and_receipt(kit):
    from contextlib import asynccontextmanager

    from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork

    created = await kit.queued()
    assert await kit.worker().once()
    before = await read_session(kit.service.uows, created.session_id)
    command = AnswerCommand(before.session.version, before.question.id, "parent")

    class FailingUnitOfWork(MySQLUnitOfWork):
        async def enqueue(self, session, answer, skip, question_id):
            await super().enqueue(session, answer, skip, question_id)
            raise RuntimeError("simulated failure after job insert")

    class FailingFactory:
        @asynccontextmanager
        async def open(self):
            async with kit.transactions.open() as db:
                yield FailingUnitOfWork(db)

    failing = InterpretationService(FailingFactory(), kit.source)
    with pytest.raises(RuntimeError, match="simulated failure"):
        await failing.answer(kit.actor, created.session_id, command, "same-key")
    assert await read_session(kit.service.uows, created.session_id) == before
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(jobs)
                .where(jobs.c.session_id == created.session_id, jobs.c.status == "queued")
            )
            == 0
        )
    result = await kit.service.answer(kit.actor, created.session_id, command, "same-key")
    assert result.status == Status.QUEUED
    assert await kit.service.answer(kit.actor, created.session_id, command, "same-key") == result


@pytest.mark.parametrize("action", ["answer", "cancel"])
async def test_change_receipt_is_replayed_before_state_validation(kit, action):
    from dataclasses import asdict

    from qs_ai.application.interpretation.service import fingerprint

    created = await kit.create()
    payload = {"expected_version": created.version}
    if action == "answer":
        payload.update(question_id=str(uuid4()), answer=None, skip=True)
    scope = fingerprint([asdict(kit.actor), action, created.session_id])
    async with kit.service.uows.open() as uow:
        await uow.reserve(scope, "receipt-key", fingerprint(payload))
        await uow.receipt(scope, "receipt-key", created)
        await uow.commit()
    # A stored receipt is returned before state/question checks and without new jobs.
    result = await getattr(kit.service, action)(
        kit.actor,
        created.session_id,
        {"answer": AnswerCommand, "cancel": CancelCommand}[action](**payload),
        "receipt-key",
    )
    assert result == created
    async with kit.transactions.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(jobs)
                .where(jobs.c.session_id == created.session_id)
            )
            == 0
        )
