"""Actual CHANGE original roots and witnesses; not Broker or model-call fault proof."""

import hashlib
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.transaction import Decision, LocalOutcome, StoreChecker
from sqlalchemy import func, insert, update

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.contracts.workflow import workflow_pb2 as workflow
from qs_ai.domain.interpretation.model import Question, Session
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, inbox, outbox
from qs_ai.infrastructure.persistence.mysql.schema import (
    jobs,
    model_calls,
    questions,
    result_outbox,
    runs,
    sessions,
)
from qs_ai.infrastructure.workflow_transport.change_plan import ChangePlanFacts
from qs_ai.infrastructure.workflow_transport.change_planner import ChangeAdmissionPlanner
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS, authenticate
from qs_ai.infrastructure.workflow_transport.transaction_admission import make_change_root
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import admission, command
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)
from tests.integration.test_transaction_start_admission import reserve, rows

pytestmark = pytest.mark.integration


async def initial(host):
    session = Session(str(uuid4()), host.kit.actor, "19", ("42",), "original target")
    session.queue(str(uuid4()))
    session.running()
    question = Question(str(uuid4()), session.id, "original question")
    session.await_answer(question.id)
    request_id = str(uuid4())
    async with host.tx.open() as db:
        uow = MySQLUnitOfWork(db)
        await uow.add(session)
        await uow.bind_request(session.id, request_id)
        await db.execute(insert(questions).values(**asdict(question), question_seq=1))
        await db.execute(
            insert(runs).values(
                id=session.active_run_id,
                session_id=session.id,
                session_version=session.version,
                status="running",
            )
        )
        await db.execute(
            insert(jobs).values(
                id=str(uuid4()),
                run_id=session.active_run_id,
                session_id=session.id,
                status="queued",
                available_at=func.utc_timestamp(6),
                fence_token=0,
                attempt=0,
                skipped=False,
            )
        )
        await db.commit()
    return SimpleNamespace(**locals())


def incoming(host, initial, action):
    command_id = str(uuid4())
    prepared = command(
        host.keys,
        pb.CHANGE,
        pb.MessagingBody(
            change=workflow.ChangeCommand(
                actor=workflow.Actor(
                    org_id=host.kit.actor.org_id, subject_id=host.kit.actor.subject_id
                ),
                command_id=command_id,
                session_id=initial.session.id,
                action=action,
                expected_version=initial.session.version,
                question_id=initial.question.id,
                answer="first answer",
            )
        ),
        initial.request_id,
        command_id=command_id,
    )
    header = authenticate(
        prepared.wire,
        COMMANDS,
        decrypt_keys={"ai.encrypt": host.keys["ai.encrypt"]},
        trusted_signers={"qs.sign": TrustedSigner("qs-server", host.keys["qs.sign"])},
    )
    value = FrozenCommandInput.from_authenticated(header, prepared.body, prepared.wire)
    host.messages.append(value)
    return value


@pytest.mark.parametrize("action", ["answer", "cancel"])
async def test_original_change_commits_business_first_state_receipt_and_all_witnesses(host, action):
    first = await initial(host)
    message = incoming(host, first, action)
    source = SimpleNamespace(authorize=AsyncMock(wraps=host.kit.source.authorize))
    candidate = await ChangeAdmissionPlanner(
        host.tx, source, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
    ).candidate(message)
    facts = ChangePlanFacts.from_plan(message, candidate)
    owned = (await reserve(host, message, candidate)).execution
    assert source.authorize.await_count == 1
    source.authorize.side_effect = AssertionError("remote client forbidden after half preparation")
    result = await make_change_root(
        host.tx, host.archives, host.store, MessagingStore(), message, owned.plan, admission(source)
    )(owned)
    assert result == LocalOutcome.COMMITTED
    assert source.authorize.await_count == 1
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.COMMIT
    current = (await rows(host, sessions, sessions.c.id == first.session.id))[0]
    assert current["version"] == 5 and current["status"] == (
        "queued" if action == "answer" else "cancelled"
    )
    history = await rows(host, result_outbox, result_outbox.c.event_id == facts.state_event_id)
    assert len(history) == 1 and history[0]["mq_owned"] and not history[0]["delivered"]
    assert history[0]["payload"]["request_id"] == first.request_id
    question = (await rows(host, questions, questions.c.id == first.question.id))[0]
    assert (question["answer"] == "first answer") is (action == "answer")
    for value in read_admission_plan(message, owned.plan):
        archive = await host.archives.read_committed(value.envelope().message_id)
        assert archive is not None
        assert hashlib.sha256(archive.wire).digest() == hashlib.sha256(value.wire).digest()
        assert not await rows(host, outbox, outbox.c.message_id == value.envelope().message_id)
    assert (await rows(host, inbox, inbox.c.message_id == message.envelope().message_id))[0][
        "decision"
    ] == "accepted"
    assert len(await rows(host, jobs, jobs.c.id == facts.job_id)) == (
        1 if action == "answer" else 0
    )
    assert not await rows(host, model_calls, model_calls.c.run_id == facts.run_id)


async def test_changed_authorization_target_aborts_whole_root_without_new_business_or_inbox(host):
    first = await initial(host)
    message = incoming(host, first, "answer")
    candidate = await ChangeAdmissionPlanner(
        host.tx,
        host.kit.source,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    ).candidate(message)
    owned = (await reserve(host, message, candidate)).execution
    facts = ChangePlanFacts.from_plan(message, owned.plan)
    async with host.tx.open() as db:
        await db.execute(
            update(sessions).where(sessions.c.id == first.session.id).values(testee_id=20)
        )
        await db.commit()
    assert (
        await make_change_root(
            host.tx,
            host.archives,
            host.store,
            MessagingStore(),
            message,
            owned.plan,
            admission(host.kit.source),
        )(owned)
        == LocalOutcome.ABORTED
    )
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.ROLLBACK
    current = (await rows(host, sessions, sessions.c.id == first.session.id))[0]
    assert current["version"] == 4 and current["status"] == "awaiting_answer"
    assert not await rows(host, jobs, jobs.c.id == facts.job_id)
    assert not await rows(host, result_outbox, result_outbox.c.event_id == facts.state_event_id)
    assert not await rows(host, inbox, inbox.c.message_id == message.envelope().message_id)


async def test_known_permission_denial_commits_only_rejection_without_external_root_call(host):
    from qs_ai.application.interpretation.ports import AccessDenied

    first = await initial(host)
    message = incoming(host, first, "cancel")
    source = SimpleNamespace(authorize=AsyncMock(side_effect=AccessDenied()))
    candidate = await ChangeAdmissionPlanner(
        host.tx, source, signing_key=host.keys["ai.sign"], recipient_key=host.keys["qs.encrypt"]
    ).candidate(message)
    owned = (await reserve(host, message, candidate)).execution
    source.authorize.side_effect = AssertionError("no remote authorization from root")
    assert (
        await make_change_root(
            host.tx,
            host.archives,
            host.store,
            MessagingStore(),
            message,
            owned.plan,
            admission(source),
        )(owned)
        == LocalOutcome.COMMITTED
    )
    assert source.authorize.await_count == 1
    assert len(owned.references) == 1
    assert await StoreChecker(host.store).check(owned.references[0]) == Decision.COMMIT
    assert (await rows(host, inbox, inbox.c.message_id == message.envelope().message_id))[0][
        "decision"
    ] == "rejected"
    assert (await rows(host, sessions, sessions.c.id == first.session.id))[0]["version"] == 4
