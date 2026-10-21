"""Explicit required infra profile, excluded from ordinary test discovery.

Real published SDK, Broker and original AI roots. QS inputs and the receiving
observer are fixtures: no paired QS business, real provider, system login,
packet fault or production acceptance is claimed.
"""

import asyncio
import hashlib
import importlib.metadata as metadata
import json
import logging
import os
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.rocketmq import Config, RocketMQSubscriber
from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
)
from reliable_messaging.transaction import (
    Binding,
    Decision,
    FinalOutcome,
    FrozenPlan,
    Limits,
    LocalOutcome,
    Request,
    StoreChecker,
    execution_key,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from reliable_messaging.transport import RouteBinding, consumer_group_v2, physical_topic_v2
from sqlalchemy import delete, event, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from qs_ai.bootstrap.transaction_messaging import TransactionRuntime
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.messaging import (
    MessagingStore,
    inbox,
    outbox,
    quarantine,
)
from qs_ai.infrastructure.persistence.mysql.transaction_schema import (
    binding,
    execution,
    request,
    witness,
)
from qs_ai.infrastructure.workflow_transport.messaging import (
    CHANNELS,
    COMMANDS,
    EVENTS,
    PreparedMessage,
    authenticate,
    parse_body,
)
from qs_ai.infrastructure.workflow_transport.state_events import evaluation_sequences
from qs_ai.infrastructure.workflow_transport.transaction_archive import freeze_branch
from qs_ai.infrastructure.workflow_transport.transaction_command import (
    FrozenCommandInput,
    read_admission_plan,
)
from qs_ai.infrastructure.workflow_transport.transaction_receiver import TransactionCommandReceiver
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_start import requested as requested
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import admission, start_message
from tests.integration.test_mq_storage import keys as keys
from tests.integration.test_mq_storage import receiver
from tests.integration.test_participant_retries import blocked, calls
from tests.integration.test_transaction_change_admission import incoming as change_input
from tests.integration.test_transaction_change_admission import initial as initial_change
from tests.integration.test_transaction_evaluation_admission import incoming as evaluation_input
from tests.integration.test_transaction_retry_admission import calls_digest
from tests.integration.test_transaction_retry_admission import incoming as retry_input
from tests.integration.test_transaction_start_admission import host as host
from tests.integration.test_transaction_start_admission import (
    required_disposable_primary as required_disposable_primary,
)

pytestmark = pytest.mark.integration
_CASES = {}
_OBSERVER = "qs-m3-ai-admission-observer"


def passed(name):
    _CASES[name] = True


@pytest.fixture(scope="session", autouse=True)
def required_broker(request):
    path = os.environ.get("QS_AI_M3_BROKER_CONFIG")
    if not path:
        pytest.fail("Required private isolated Broker profile missing")
    config = json.loads(Path(path).read_text())
    assert config["endpoint"].startswith("127.0.0.1:")
    assert metadata.version("fangcun-reliable-messaging") == "0.4.0a1"
    assert metadata.version("rocketmq-python-client") == "5.1.2"
    import reliable_messaging

    assert Path(reliable_messaging.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    # Pinned upstream opens a default handler on import. No client/key exists
    # yet: replace only this isolated process's log sink before any RPC.
    from rocketmq.v5.log.log_config import logger

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    private_log = Path(config["client_log"])
    private_log.touch(mode=0o600)
    logger.addHandler(logging.FileHandler(private_log))
    logger.setLevel(logging.ERROR)
    try:
        yield config
    finally:
        print(
            "M3_AI_ADMISSION_RECEIPT "
            + json.dumps(
                {
                    "cases": _CASES,
                    "passed": len(_CASES),
                    "failed": request.session.testsfailed,
                    "skipped": len(
                        request.config.pluginmanager.get_plugin("terminalreporter").stats.get(
                            "skipped", []
                        )
                    ),
                    "root": "original_mysql_sqlalchemy",
                    "sdk": "0.4.0a1",
                    "client": "5.1.2",
                    "python": sys.version.split()[0],
                    "system_login_not_claimed": True,
                    "paired_qs_business_not_claimed": True,
                    "real_provider_not_claimed": True,
                    "process_exit_not_claimed": True,
                    "database_commit_network_fault_not_claimed": True,
                }
            ),
            flush=True,
        )
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()


class ObservedRuntime(TransactionRuntime):
    def __init__(self, sender, host):
        super().__init__(sender, admitted=True)
        self.host, self.results, self.executions = host, [], []
        self.pause = False
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.completed = asyncio.Event()

    async def reserve(self, request_value, candidate):
        reserved = await super().reserve(request_value, candidate)
        if reserved.acquired:
            self.host.executions.append(reserved.execution)
        return reserved

    async def execute(self, owned, local):
        async def original_root(exact):
            key = execution_key(
                exact.plan.owner, exact.plan.operation_id, exact.writer_epoch, exact.fence
            )
            async with self.host.tx.open() as db:
                states = (
                    await db.scalars(
                        select(witness.c.half_outcome).where(witness.c.execution_key == key)
                    )
                ).all()
            assert len(states) == len(exact.references) and all(
                bytes(s) == b"prepared" for s in states
            )
            passed("all_halves_before_local")
            self.entered.set()
            if self.pause:
                await self.release.wait()
            return await local(exact)

        self.executions.append(owned)
        result = await super().execute(owned, original_root)
        self.results.append(result)
        self.completed.set()
        return result


@pytest.fixture
async def broker(host, required_broker):
    config = required_broker
    command_topic = physical_topic_v2(COMMANDS, "TRANSACTION", "qs.mysql", "m3-1")
    event_topic = physical_topic_v2(EVENTS, "TRANSACTION", "ai.mysql", "m3-1")
    command_route = RouteBinding(
        COMMANDS,
        CHANNELS[COMMANDS],
        command_topic,
        consumer_group_v2(COMMANDS, CHANNELS[COMMANDS]),
        "TRANSACTION",
        "qs.mysql",
        "m3-1",
    )
    event_route = RouteBinding(
        EVENTS,
        _OBSERVER,
        event_topic,
        consumer_group_v2(EVENTS, _OBSERVER),
        "TRANSACTION",
        "ai.mysql",
        "m3-1",
    )
    sessions = async_sessionmaker(
        host.tx.database.engine, class_=AsyncSession, expire_on_commit=False
    )
    qs_binding = Binding("qs.mysql", "input-fixture-" + uuid4().hex, "m3-1")
    qs_store = MySQLTransactionStore(sessions, qs_binding)
    qs_executions, messages, failures, statement_count = [], {}, [], []
    empty_poll = asyncio.Event()
    loop = asyncio.get_running_loop()
    async with sessions() as db, db.begin():
        await db.execute(
            insert(binding).values(
                owner=b"qs.mysql",
                store_id=qs_binding.store_id.encode(),
                writer_epoch=b"m3-1",
                schema_version=1,
            )
        )
    owner = admission(host.kit.source)
    boundary = receiver(host.tx, MessagingStore(), host.keys, owner)
    limits = Limits(sends=4, check_workers=1, check_queue=16)
    ai_credentials, qs_credentials, observer_credentials = (
        config[role] for role in ("ai", "input", "observer")
    )

    def count_sql(*_):
        statement_count.append(True)

    event.listen(host.tx.database.engine.sync_engine, "before_cursor_execute", count_sql)
    try:
        sender = TransactionalSender(
            TransactionConfig(
                config["endpoint"],
                ai_credentials["username"],
                ai_credentials["password"],
                "ai.mysql",
                (TransactionRoute(EVENTS, event_topic, "m3-1"),),
                limits,
            ),
            host.store,
        )
        runtime = ObservedRuntime(sender, host)
        command_receiver = TransactionCommandReceiver(boundary, owner, runtime, host.archives)
        input_sender = TransactionalSender(
            TransactionConfig(
                config["endpoint"],
                qs_credentials["username"],
                qs_credentials["password"],
                "qs.mysql",
                (TransactionRoute(COMMANDS, command_topic, "m3-1"),),
                limits,
            ),
            qs_store,
        )
        assert not statement_count and sender._client is None and input_sender._client is None
        passed("constructor_inert")
    finally:
        event.remove(host.tx.database.engine.sync_engine, "before_cursor_execute", count_sql)

    async def failed(_):
        failures.append("unexpected_physical_failure")
        raise RuntimeError("Unexpected isolated physical failure")

    async def invalid(_, code):
        failures.append("unexpected_invalid_wire")
        raise RuntimeError("Unexpected isolated invalid wire")

    async def observe(delivery):
        header = authenticate(
            delivery.wire,
            EVENTS,
            decrypt_keys={"qs.encrypt": host.keys["qs.encrypt"]},
            trusted_signers={"ai.sign": TrustedSigner("qs-ai", host.keys["ai.sign"])},
        )
        archived = await host.archives.read_committed(header.message_id)
        assert (
            archived is not None
            and hashlib.sha256(archived.wire).digest() == hashlib.sha256(delivery.wire).digest()
        )
        parse_body(header, archived.body)
        digest = hashlib.sha256(delivery.wire).hexdigest()
        assert header.message_id not in messages or messages[header.message_id] == digest
        messages[header.message_id] = digest

    source = RocketMQSubscriber(
        Config(
            config["endpoint"],
            ai_credentials["username"],
            ai_credentials["password"],
            (command_route,),
            max_in_flight=1,
        ),
        COMMANDS,
        CHANNELS[COMMANDS],
        handler=command_receiver.receive_command,
        failed_handler=command_receiver.failed_command,
        invalid_handler=command_receiver.invalid_wire,
        retry_cap=1,
    )
    observer = RocketMQSubscriber(
        Config(
            config["endpoint"],
            observer_credentials["username"],
            observer_credentials["password"],
            (event_route,),
            max_in_flight=1,
        ),
        EVENTS,
        _OBSERVER,
        handler=observe,
        failed_handler=failed,
        invalid_handler=invalid,
        retry_cap=1,
    )

    async def send(incoming):
        if incoming not in host.messages:
            host.messages.append(incoming)
        prepared = PreparedMessage(incoming.envelope(), incoming.body, incoming.wire, COMMANDS)
        branch = freeze_branch(
            prepared,
            incoming.organization_id,
            1,
            False,
            False,
            incoming.envelope().message_id,
            command_topic,
            "m3-1",
        )
        original = Request(
            "qs.mysql",
            "org:" + incoming.organization_id,
            "fixture-input/" + incoming.envelope().message_id,
            hashlib.sha256(incoming.wire).digest(),
        )
        candidate = FrozenPlan(
            original.owner,
            original.scope,
            str(uuid4()),
            original.key,
            original.input_hash,
            (branch,),
        )
        reserved = await input_sender.reserve(original, candidate)
        assert reserved.acquired
        qs_executions.append(reserved.execution)

        async def commit_input(exact):
            # Controlled QS input fixture only, not a QS business fact.
            async with sessions() as db, db.begin():
                guard = await qs_store.bind(db, exact)
                await guard.commit_witness()
            return LocalOutcome.COMMITTED

        result = await input_sender.execute(reserved.execution, commit_input)
        assert result.local == LocalOutcome.COMMITTED

    async def complete(incoming, expected_branches=2):
        async with asyncio.timeout(60):
            while True:
                assert not failures
                observed = await host.store.lookup(incoming.request)
                if observed is not None and observed.state.value == "committed":
                    values = read_admission_plan(incoming, observed.execution.plan)
                    if len(values) == expected_branches and all(
                        v.envelope().message_id in messages for v in values
                    ):
                        for ref in observed.execution.references:
                            assert await StoreChecker(host.store).check(ref) == Decision.COMMIT
                        async with host.tx.open() as db:
                            ids = [v.envelope().message_id for v in values]
                            assert not (
                                await db.scalars(
                                    select(outbox.c.message_id).where(outbox.c.message_id.in_(ids))
                                )
                            ).all()
                        passed("no_new_publish_outbox")
                        passed("fresh_checker")
                        passed("original_first_wire")
                        return observed
                await asyncio.sleep(0.05)

    resources = AsyncExitStack()
    try:
        await observer.start()
        resources.push_async_callback(observer.stop, grace_seconds=20)
        original_receive = observer._driver.client.receive

        def observed_receive(*args, **kwargs):
            values = original_receive(*args, **kwargs)
            if not values:
                loop.call_soon_threadsafe(empty_poll.set)
            return values

        observer._driver.client.receive = observed_receive
        await input_sender.start()
        resources.push_async_callback(input_sender.close, grace_seconds=20)
        await runtime.start()
        resources.push_async_callback(runtime.close, grace_seconds=20)
        await source.start()
        resources.push_async_callback(source.stop, grace_seconds=20)
        yield SimpleNamespace(**locals())
    finally:
        runtime.release.set()
        try:
            await resources.aclose()
        finally:
            async with sessions() as db, db.begin():
                for owned in qs_executions:
                    key = execution_key(
                        owned.plan.owner, owned.plan.operation_id, owned.writer_epoch, owned.fence
                    )
                    for table in (witness, execution):
                        await db.execute(delete(table).where(table.c.execution_key == key))
                    original = Request(
                        owned.plan.owner,
                        owned.plan.scope,
                        owned.plan.idempotency_key,
                        owned.plan.input_hash,
                    )
                    await db.execute(
                        delete(request).where(request.c.request_key == original.key_digest)
                    )
                await db.execute(
                    delete(binding).where(
                        binding.c.owner == b"qs.mysql",
                        binding.c.store_id == qs_binding.store_id.encode(),
                    )
                )
                for incoming in host.messages:
                    await db.execute(
                        delete(quarantine).where(
                            quarantine.c.logical_producer == "qs-server",
                            quarantine.c.logical_message_id == incoming.envelope().message_id,
                        )
                    )


def frozen_start(host):
    value = start_message(host.kit, host.keys)
    header = authenticate(
        value.wire,
        COMMANDS,
        decrypt_keys={"ai.encrypt": host.keys["ai.encrypt"]},
        trusted_signers={"qs.sign": TrustedSigner("qs-server", host.keys["qs.sign"])},
    )
    return FrozenCommandInput.from_authenticated(header, value.body, value.wire)


@pytest.mark.usefixtures("published_configuration")
async def test_actual_start_half_gate_and_duplicate_reuses_original_plan(broker):
    broker.runtime.pause = True
    incoming = frozen_start(broker.host)
    await broker.send(incoming)
    await asyncio.wait_for(broker.runtime.entered.wait(), timeout=30)
    broker.empty_poll.clear()
    await asyncio.wait_for(broker.empty_poll.wait(), timeout=20)
    assert not broker.messages
    passed("half_invisible")
    broker.runtime.release.set()
    first = await broker.complete(incoming)
    assert len(broker.runtime.executions) == 1
    await broker.command_receiver.receive_command(
        SimpleNamespace(
            wire=incoming.wire,
            provider="rocketmq",
            logical_topic=COMMANDS,
            physical_topic=broker.command_topic,
        )
    )
    again = await broker.host.store.lookup(incoming.request)
    assert (
        first.execution.plan.digest == again.execution.plan.digest
        and len(broker.runtime.executions) == 1
    )
    passed("duplicate_no_reseal")
    passed("start")


@pytest.mark.parametrize("action", ["answer", "cancel"])
async def test_actual_change_original_root_over_broker(broker, action):
    first = await initial_change(broker.host)
    incoming = change_input(broker.host, first, action)
    await broker.send(incoming)
    await broker.complete(incoming)
    passed("change_" + action)


@pytest.mark.usefixtures("published_configuration")
async def test_actual_participant_retry_does_not_add_model_invocations(broker):
    value, _ = await blocked(broker.host.kit)
    before = calls_digest(await calls(broker.host.kit, value.session_id))
    incoming = await retry_input(broker.host, value)
    await broker.send(incoming)
    await broker.complete(incoming)
    assert calls_digest(await calls(broker.host.kit, value.session_id)) == before
    passed("participant_retry")


@pytest.mark.parametrize("kind", [pb.EVALUATION_START, pb.EVALUATION_CANCEL])
async def test_actual_evaluation_original_root_over_broker(broker, requested, kind):
    _, scope, store = requested
    try:
        incoming = evaluation_input(broker.host, scope, kind)
        await broker.send(incoming)
        await broker.complete(incoming)
        view = await store.get(scope)
        assert view.version == 2 and view.status == (
            "collecting" if kind == pb.EVALUATION_START else "canceled"
        )
        passed("evaluation_start" if kind == pb.EVALUATION_START else "evaluation_cancel")
    finally:
        async with broker.host.tx.open() as db:
            await db.execute(
                delete(evaluation_sequences).where(
                    evaluation_sequences.c.run_id == str(scope.run_id)
                )
            )
            await db.commit()


async def test_actual_local_technical_budget_only_publishes_one_held_receipt(broker):
    incoming = frozen_start(broker.host)
    async with broker.host.tx.open() as db, db.begin():
        for _ in range(8):
            await MessagingStore().technical_failure(
                db, incoming.envelope(), incoming.body, incoming.wire
            )
    await broker.send(incoming)
    await broker.complete(incoming, expected_branches=1)
    async with broker.host.tx.open() as db:
        decision = await db.scalar(
            select(inbox.c.decision).where(inbox.c.message_id == incoming.envelope().message_id)
        )
    assert decision == "held"
    passed("technical_hold")


@pytest.mark.usefixtures("published_configuration")
async def test_real_commit_reply_deliberately_discarded_keeps_local_commit_and_first_wire(
    broker, monkeypatch
):
    real_end = broker.sender._client.end_transaction

    def discard_reply(*args, **kwargs):
        real_end(*args, **kwargs)
        raise TimeoutError("client discarded completed final reply")

    monkeypatch.setattr(broker.sender._client, "end_transaction", discard_reply)
    incoming = frozen_start(broker.host)
    await broker.send(incoming)
    await broker.complete(incoming)
    async with asyncio.timeout(10):
        await broker.runtime.completed.wait()
    result = broker.runtime.results[-1]
    assert result.local == LocalOutcome.COMMITTED and all(
        branch.final == FinalOutcome.UNKNOWN for branch in result.branches
    )
    passed("lost_final_reply")
