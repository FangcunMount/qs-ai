"""Real command receiver/original SQLAlchemy COMMIT TCP faults; explicitly selected only."""

import asyncio
import json
import os
import subprocess
from types import SimpleNamespace

import pytest
from reliable_messaging.rocketmq_transaction import TransactionalSender
from reliable_messaging.transaction import Decision, State, StoreChecker
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.infrastructure.persistence.mysql.messaging import inbox, outbox
from qs_ai.infrastructure.persistence.mysql.schema import model_calls, sessions
from qs_ai.infrastructure.workflow_transport.messaging import COMMANDS
from qs_ai.infrastructure.workflow_transport.start_plan import StartPlanFacts
from qs_ai.infrastructure.workflow_transport.transaction_command import read_admission_plan
from tests.integration.rocketmq_admission_host import broker as broker
from tests.integration.rocketmq_admission_host import frozen_start
from tests.integration.rocketmq_admission_host import host as host
from tests.integration.rocketmq_admission_host import keys as keys
from tests.integration.rocketmq_admission_host import kit as kit
from tests.integration.rocketmq_admission_host import required_broker as required_broker
from tests.integration.rocketmq_admission_host import (
    required_disposable_primary as required_disposable_primary,
)
from tests.route_fixtures import legacy_routes

pytestmark = pytest.mark.integration


async def eventually(predicate, *, seconds=100):
    async with asyncio.timeout(seconds):
        while not await predicate():  # noqa: ASYNC110 - External DB/Broker has no event hook.
            await asyncio.sleep(0.1)


@pytest.mark.usefixtures("published_configuration")
async def test_actual_command_original_commit_packet_fault(broker):
    boundary = os.environ["QS_AI_M3_FAULT_BOUNDARY"]
    assert boundary in {"before", "after"}
    primary = os.environ["QS_AI_M3_FAULT_MYSQL_PRIMARY"]
    primary_host, primary_port = primary.rsplit(":", 1)
    assert primary_host == "127.0.0.1" and 1 <= int(primary_port) <= 65535
    original_url = make_url(os.environ["QS_AI_TEST_MYSQL_DSN"])
    assert original_url.host == "127.0.0.1" and original_url.port != int(primary_port)
    primary_url = original_url.set(
        drivername="mysql+asyncmy", host=primary_host, port=int(primary_port)
    )
    fresh_engine = create_async_engine(primary_url, pool_size=2, max_overflow=0)
    fresh_sessions = async_sessionmaker(fresh_engine, expire_on_commit=False)
    authority = MySQLTransactionStore(fresh_sessions, broker.host.store.binding)
    incoming = frozen_start(broker.host)
    broker.host.messages.append(incoming)
    delivery = SimpleNamespace(
        wire=incoming.wire,
        provider="rocketmq",
        logical_topic=COMMANDS,
        physical_topic=broker.command_topic,
    )
    local_calls = 0
    original_execute = broker.runtime.execute

    async def measured_execute(owned, local):
        async def measured_original_root(exact):
            nonlocal local_calls
            local_calls += 1
            return await local(exact)

        return await original_execute(owned, measured_original_root)

    # Instance-only observation. It neither injects an error nor replaces the
    # original receiver/root, SDK, database transaction or callback result.
    broker.runtime.execute = measured_execute
    next_runtime = None
    try:
        error = None
        try:
            await broker.command_receiver.receive_command(delivery)
        except Exception as caught:
            error = caught
        assert local_calls == 1 and len(broker.runtime.executions) == 1
        winner = await authority.lookup(incoming.request)
        assert winner is not None and len(winner.execution.references) == 2
        frozen = winner.execution.plan
        facts = StartPlanFacts.from_plan(incoming, frozen)
        archives = read_admission_plan(incoming, frozen)
        async with fresh_sessions() as db:
            business_rows = await db.scalar(
                select(func.count()).select_from(sessions).where(sessions.c.id == facts.session_id)
            )
            inbox_rows = await db.scalar(
                select(func.count())
                .select_from(inbox)
                .where(inbox.c.message_id == incoming.envelope().message_id)
            )
            invocations = await db.scalar(
                select(func.count())
                .select_from(model_calls)
                .where(model_calls.c.run_id == facts.run_id)
            )
        decisions = [
            await StoreChecker(authority).check(ref) for ref in winner.execution.references
        ]
        if boundary == "after":
            assert error is None and business_rows == inbox_rows == 1
            assert all(decision == Decision.COMMIT for decision in decisions)
        else:
            assert error is not None and business_rows == inbox_rows == 0
            assert all(decision != Decision.COMMIT for decision in decisions)
        assert invocations == 0
        duplicate_error = None
        try:
            await broker.command_receiver.receive_command(delivery)
        except Exception as caught:
            duplicate_error = caught
        assert (duplicate_error is None) == (boundary == "after")
        duplicate = await authority.lookup(incoming.request)
        assert duplicate is not None and duplicate.execution.plan.digest == frozen.digest
        assert local_calls == 1 and len(broker.runtime.executions) == 1
        assert broker.runtime.stats()["half_prepared_results"] == 2
        await broker.runtime.close(grace_seconds=20)
        # A paused replacement uses an independent short-lived session factory
        # on the original primary. No new business operation is authorized.
        next_runtime = TransactionRuntime(
            TransactionalSender(broker.sender.config, authority),
            admitted=False,
            routing=legacy_routes(),
        )
        await next_runtime.start()

        async def resolved():
            observed = await authority.lookup(incoming.request)
            decisions = [
                await StoreChecker(authority).check(ref) for ref in winner.execution.references
            ]
            if boundary == "after":
                return observed.state == State.COMMITTED and all(
                    d == Decision.COMMIT for d in decisions
                )
            return (
                observed.state == State.ABORTED
                and all(d == Decision.ROLLBACK for d in decisions)
                and next_runtime.stats()["check_rollback"] > 0
            )

        await eventually(resolved)
        if boundary == "after":

            async def visible():
                return all(value.envelope().message_id in broker.messages for value in archives)

            await eventually(visible, seconds=45)
            for value in archives:
                assert (
                    await broker.host.archives.read_committed(value.envelope().message_id) == value
                )
        else:
            await asyncio.sleep(2)
            assert not broker.messages
            for value in archives:
                assert (
                    await broker.host.archives.read_committed(value.envelope().message_id) is None
                )
        stats = next_runtime.stats()
        assert stats["half_prepared_results"] == 0 and not stats["ready"]
        async with fresh_sessions() as db:
            assert not (
                await db.scalars(
                    select(outbox.c.message_id).where(
                        outbox.c.message_id.in_([value.envelope().message_id for value in archives])
                    )
                )
            ).all()
        await next_runtime.close(grace_seconds=20)
        async with fresh_sessions() as db:
            assert await db.scalar(select(1)) == 1
        cases = dict(
            actual_command_root_once=True,
            business_inbox_witness_match_boundary=True,
            duplicate_keeps_original_plan_and_result=True,
            broker_visibility_or_new_paused_checker=True,
            published_outbox_empty_provider_invocations_zero=True,
            owned_runtimes_drained_primary_retained=True,
        )
        print(
            "M3_AI_DATABASE_FAULT_RECEIPT "
            + json.dumps(
                dict(
                    source_sha=(
                        await asyncio.to_thread(
                            subprocess.check_output, ["git", "rev-parse", "HEAD"], text=True
                        )
                    ).strip(),
                    boundary=boundary,
                    scope=(
                        "actual AI command receiver original SQLAlchemy root "
                        "with wire-level MySQL COMMIT faults; component only"
                    ),
                    passed=6,
                    failed=0,
                    skipped=0,
                    cases=cases,
                    local_calls=local_calls,
                    half_prepared=broker.runtime.stats()["half_prepared_results"],
                    new_half_prepared=stats["half_prepared_results"],
                    new_check_rollback=stats["check_rollback"],
                    business_rows=business_rows,
                    inbox_rows=inbox_rows,
                    provider_invocations=invocations,
                    fixed_two_branch_wire=True,
                    full_api_process_real_provider_system_not_claimed=True,
                )
            ),
            flush=True,
        )
    finally:
        if next_runtime is not None:
            await next_runtime.close(grace_seconds=20)
        await fresh_engine.dispose()
