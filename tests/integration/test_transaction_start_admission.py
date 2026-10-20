"""Actual original START database roots; not Broker/half-message or packet-fault proof."""

import asyncio
import hashlib
import os
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.transaction import (
    Binding,
    Decision,
    LocalOutcome,
    ReplanAuthorization,
    State,
    StoreChecker,
    execution_key,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy import delete, func, insert, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker

from qs_ai.application.governance.publication import MovePublication
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore, inbox, outbox
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.schema import (
    jobs,
    model_calls,
    result_outbox,
    runs,
    sessions,
)
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.persistence.mysql.transaction_schema import (
    archive,
    binding,
    execution,
    request,
    witness,
)
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS, authenticate
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from qs_ai.infrastructure.workflow_transport.start_planner import StartAdmissionPlanner
from qs_ai.infrastructure.workflow_transport.transaction_admission import make_start_root
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import admission, start_message
from tests.integration.test_mq_storage import keys as keys

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def required_disposable_primary():
    dsn = os.environ.get("QS_AI_TEST_MYSQL_DSN")
    if not dsn or make_url(dsn).host not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("Required explicitly disposable loopback QS_AI_TEST_MYSQL_DSN missing")


@pytest.fixture
async def host(kit, keys):
    tx = kit.transactions
    binding_value = Binding("ai.mysql", "start-root-" + uuid4().hex, "m3-1")
    factory = async_sessionmaker(tx.database.engine, expire_on_commit=False)
    store = MySQLTransactionStore(factory, binding_value)
    archives = TransactionArchive(factory, binding_value)
    messages, executions = [], []
    try:
        async with tx.open() as db:
            assert (
                await db.scalar(select(binding.c.owner).where(binding.c.owner == b"ai.mysql"))
                is None
            )
            await db.execute(
                insert(binding).values(
                    owner=b"ai.mysql",
                    store_id=binding_value.store_id.encode(),
                    writer_epoch=b"m3-1",
                    schema_version=1,
                )
            )
            await db.commit()
        await store.validate()
        yield SimpleNamespace(
            kit=kit,
            keys=keys,
            tx=tx,
            store=store,
            archives=archives,
            messages=messages,
            executions=executions,
        )
    finally:
        async with tx.open() as db:
            request_keys = set()
            for owned in executions:
                key = execution_key(
                    owned.plan.owner, owned.plan.operation_id, owned.writer_epoch, owned.fence
                )
                for table in (archive, witness, execution):
                    await db.execute(delete(table).where(table.c.execution_key == key))
                request_keys.add(
                    (
                        owned.plan.owner,
                        owned.plan.scope,
                        owned.plan.idempotency_key,
                        owned.plan.input_hash,
                    )
                )
            from reliable_messaging.transaction import Request

            for owner, scope, key, input_hash in request_keys:
                value = Request(owner, scope, key, input_hash)
                await db.execute(delete(request).where(request.c.request_key == value.key_digest()))
            for message in messages:
                await db.execute(
                    delete(inbox).where(
                        inbox.c.producer == "qs-server",
                        inbox.c.message_id == message.envelope().message_id,
                    )
                )
            await db.execute(
                delete(binding).where(
                    binding.c.owner == b"ai.mysql",
                    binding.c.store_id == binding_value.store_id.encode(),
                )
            )
            await db.commit()


def incoming(host):
    message = start_message(host.kit, host.keys)
    header = authenticate(
        message.wire,
        COMMANDS,
        decrypt_keys={"ai.encrypt": host.keys["ai.encrypt"]},
        trusted_signers={"qs.sign": TrustedSigner("qs-server", host.keys["qs.sign"])},
    )
    value = FrozenCommandInput.from_authenticated(header, message.body, message.wire)
    host.messages.append(value)
    return value


def planner(host):
    return StartAdmissionPlanner(
        host.tx,
        host.kit.source,
        signing_key=host.keys["ai.sign"],
        recipient_key=host.keys["qs.encrypt"],
    )


async def rows(host, table, condition):
    async with host.tx.open() as db:
        return (await db.execute(select(table).where(condition))).mappings().all()


async def reserve(host, message, candidate):
    value = await host.store.reserve(message.request, candidate)
    if value.acquired:
        host.executions.append(value.execution)
    return value


async def assert_committed_original_root(host, message, owned, expected_status):
    plan = owned.plan
    facts = StartPlanFacts.from_plan(message, plan)
    local = make_start_root(
        host.tx,
        host.archives,
        host.store,
        MessagingStore(),
        message,
        plan,
        admission(host.kit.source),
    )
    assert await local(owned) == LocalOutcome.COMMITTED
    # Fresh authoritative boundary per reference, not business-row existence.
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.COMMIT
    state_rows = await rows(host, sessions, sessions.c.id == facts.session_id)
    assert len(state_rows) == 1 and state_rows[0]["status"] == expected_status
    result_rows = await rows(host, result_outbox, result_outbox.c.event_id == facts.state_event_id)
    assert len(result_rows) == 1 and result_rows[0]["mq_owned"] is True
    assert result_rows[0]["delivered"] is False and result_rows[0]["attempts"] == 0
    original_inbox = await rows(host, inbox, inbox.c.message_id == message.envelope().message_id)
    assert len(original_inbox) == 1
    assert original_inbox[0]["decision"] == (
        "accepted" if expected_status == "queued" else "rejected"
    )
    for value in read_admission_plan(message, plan):
        stored = await host.archives.read_committed(value.envelope().message_id)
        assert stored is not None
        assert hashlib.sha256(stored.wire).hexdigest() == hashlib.sha256(value.wire).hexdigest()
        assert hashlib.sha256(stored.body).hexdigest() == hashlib.sha256(value.body).hexdigest()
        assert not await rows(host, outbox, outbox.c.message_id == value.envelope().message_id)
    async with host.tx.open() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(model_calls)
                .where(model_calls.c.run_id == facts.run_id)
            )
            == 0
        )
        # Even the exact prior frozen token cannot reacquire an accepted/rejected command.
        duplicate = await MessagingStore().reserve_command(
            db,
            message.envelope(),
            message.body,
            message.wire,
            reservation_token=facts.reservation_token,
        )
        assert duplicate is not None and duplicate["receipt_id"] == original_inbox[0]["receipt_id"]
        await db.rollback()
    return facts


@pytest.mark.usefixtures("published_configuration")
async def test_concurrent_complete_candidates_have_one_winner_and_one_original_start(host):
    message = incoming(host)
    first, second = await planner(host).candidate(message), await planner(host).candidate(message)
    assert first.digest != second.digest
    reservations = await asyncio.gather(
        reserve(host, message, first), reserve(host, message, second)
    )
    assert sum(r.acquired for r in reservations) == 1
    winner = next(r.execution for r in reservations if r.acquired)
    assert all(r.execution.plan.digest == winner.plan.digest for r in reservations)
    facts = await assert_committed_original_root(host, message, winner, "queued")
    assert len(await rows(host, jobs, jobs.c.id == facts.job_id)) == 1
    assert len(await rows(host, runs, runs.c.id == facts.run_id)) == 1
    loser = first if winner.plan.digest != first.digest else second
    loser_facts = StartPlanFacts.from_plan(message, loser)
    assert not await rows(host, sessions, sessions.c.id == loser_facts.session_id)
    observed = await host.store.lookup(message.request)
    assert (
        observed.state == State.COMMITTED and not observed.acquired and not observed.execution.owned
    )


async def test_original_configuration_refusal_commits_history_receipt_and_all_witnesses(host):
    message = incoming(host)
    candidate = await planner(host).candidate(message)
    owned = (await reserve(host, message, candidate)).execution
    facts = await assert_committed_original_root(host, message, owned, "blocked")
    assert not await rows(host, jobs, jobs.c.run_id == facts.run_id)
    assert len(await rows(host, runs, runs.c.id == facts.run_id)) == 1


async def test_publication_change_aborts_original_root_and_explicit_replan_keeps_old_proof(
    host, published_configuration, ready
):
    message = incoming(host)
    candidate = await planner(host).candidate(message)
    owned = (await reserve(host, message, candidate)).execution
    previous_facts = StartPlanFacts.from_plan(message, owned.plan)
    tx, scope, command, at = ready
    active = published_configuration.change.current.active.publication_id
    await MySQLPublications(tx).apply(
        scope,
        MovePublication(
            uuid4(), command.selector, 1, active, "isolated pause before original root", True, None
        ),
        at + timedelta(seconds=1),
    )
    local = make_start_root(
        host.tx,
        host.archives,
        host.store,
        MessagingStore(),
        message,
        owned.plan,
        admission(host.kit.source),
    )
    assert await local(owned) == LocalOutcome.ABORTED
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.ROLLBACK
    assert not await rows(host, sessions, sessions.c.id == previous_facts.session_id)
    assert not await rows(host, inbox, inbox.c.message_id == message.envelope().message_id)
    assert not await rows(
        host, archive, archive.c.message_id == owned.plan.branches[0].message.message_id.encode()
    )
    # Explicit authenticated host authorization, never SDK/business automatic retry.
    next_candidate = await planner(host).candidate(message)
    advanced = await host.store.replan(ReplanAuthorization(owned, next_candidate))
    assert advanced.acquired
    host.executions.append(advanced.execution)
    await assert_committed_original_root(host, message, advanced.execution, "blocked")
    for reference in owned.references:
        assert await StoreChecker(host.store).check(reference) == Decision.ROLLBACK
