"""Explicit embedded MQ assembly on the unified application's loop and database."""

import asyncio
import os
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING

from dishka import AsyncContainer
from reliable_messaging.nsq import NSQPublisher, NSQSubscriber
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.rocketmq import (
    RocketMQPublisher,
    RocketMQSubscriber,
)
from reliable_messaging.transaction import ShutdownIncompleteError
from reliable_messaging.transport import Publisher, Subscriber
from sqlalchemy import text
from tornado.httpclient import AsyncHTTPClient

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import EditableModelPolicy
from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.bootstrap.lifecycle import Component, RuntimeState, event
from qs_ai.bootstrap.messaging_config import read_key as read_key
from qs_ai.bootstrap.messaging_config import read_rocketmq_config as read_rocketmq_config
from qs_ai.config import Settings
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.persistence.mysql.messaging_observations import require_recording_schema
from qs_ai.infrastructure.persistence.mysql.transaction_archive import ArchivePayloadReader
from qs_ai.infrastructure.persistence.mysql.transaction_receipts import TransactionMessagingStore
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.messaging import ACKS, CHANNELS, COMMANDS, EVENTS
from qs_ai.infrastructure.workflow_transport.mq_failure import FailureTopology
from qs_ai.infrastructure.workflow_transport.mq_receiver import CommandReceiver
from qs_ai.infrastructure.workflow_transport.mq_relay import MQRelay
from qs_ai.infrastructure.workflow_transport.payload_access import MySQLPayloadAccess, PayloadReader
from qs_ai.infrastructure.workflow_transport.payloads import PayloadResolver
from qs_ai.infrastructure.workflow_transport.state_events import StateEventRecorder
from qs_ai.infrastructure.workflow_transport.transaction_events import TransactionOnlyEvents
from qs_ai.infrastructure.workflow_transport.transaction_receiver import TransactionCommandReceiver
from qs_ai.transport.grpc.message_payloads import MessagePayloads

if TYPE_CHECKING:
    from qs_ai.bootstrap.transaction_services import TransactionServices


class MessagingRuntime:
    recorder: StateEventRecorder | TransactionOnlyEvents
    tx: Transactions
    relay: MQRelay | None = None
    payloads: MessagePayloads

    def __init__(self) -> None:
        self.resources = AsyncExitStack()
        self.publish_enabled = True
        self.publishers: dict[str, Publisher] = {}
        self.subscribers: list[Subscriber] = []
        self.publishers_started: set[str] = set()
        self.publisher_ready = asyncio.Event()
        self.database: Database | None = None
        self.http: AsyncHTTPClient | None = None
        self.transaction_services: TransactionServices | None = None
        self._transaction_closed = asyncio.Event()
        self._close_task: asyncio.Task[None] | None = None

    @classmethod
    async def create(
        cls, container: AsyncContainer, settings: Settings, ca: bytes, cert: bytes, key: bytes
    ) -> "MessagingRuntime":
        runtime = cls()
        try:
            options = settings.messaging
            if options.transactions and container is None:
                raise ValueError(
                    "Transaction owners require transaction-bound workflow assembly; "
                    "legacy MQRelay cannot publish them"
                )
            runtime.publish_enabled = options.publish_enabled
            if not options.enabled or not settings.grpc.access_address:
                raise ValueError("MQ requires the configured QS mTLS payload endpoint")
            assert options.signing_key_file and options.qs_recipient_key_file
            signing = await asyncio.to_thread(read_key, options.signing_key_file, private=True)
            recipient = await asyncio.to_thread(
                read_key, options.qs_recipient_key_file, private=False
            )
            decrypt = {
                kid: await asyncio.to_thread(read_key, path, private=True, expected_id=kid)
                for kid, path in options.decrypt_key_files.items()
            }
            signers = {
                kid: TrustedSigner(
                    "qs-server",
                    await asyncio.to_thread(read_key, path, private=False, expected_id=kid),
                )
                for kid, path in options.qs_signer_files.items()
            }
            database = await container.get(Database)
            runtime.database = database
            tx, store = Transactions(database), MessagingStore()
            if options.transactions:
                # Import only at explicit assembly; the provider's inert factory
                # itself imports this module's host-owned file readers.
                from qs_ai.bootstrap.transaction_services import TransactionServices

                runtime.transaction_services = await container.get(TransactionServices | None)
                if runtime.transaction_services is None:
                    raise ValueError("Transaction-bound application services required")
                services = runtime.transaction_services
                signing, recipient = services.signing_key, services.recipient_key
                store = TransactionMessagingStore(
                    services.archives, database_seconds=services.database_seconds
                )
            # Borrow the application pool before any transport or subscription exists.
            # The read is rolled back by Transactions; startup never installs its schema.
            async with asyncio.timeout(5), tx.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                await db.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
                await require_recording_schema(db)
            runtime.recorder = (
                TransactionOnlyEvents()
                if runtime.transaction_services is not None
                else StateEventRecorder(store, signing, recipient)
            )
            database.state_events = runtime.recorder
            async with container() as scope:
                admission = WorkflowCommandAdmission(
                    await scope.get(EvidenceSource),
                    await scope.get(ParticipantCapacityPolicy),
                    await scope.get(EvaluationCapacityPolicy),
                    await scope.get(EditableModelPolicy),
                    await scope.get(QuotaBaseline),
                    await scope.get(EvaluationRuntimeLimits),
                )
            channel = await runtime.resources.enter_async_context(
                mtls_channel(settings.grpc.access_address, ca, key, cert)
            )
            resolver = PayloadResolver({"qs-server": rpc.MessagePayloadsStub(channel)})
            receiver = CommandReceiver(
                tx,
                store,
                admission,
                resolver,
                decrypt_keys=decrypt,
                trusted_signers=signers,
                signing_key=signing,
                qs_recipient_key=recipient,
            )
            if options.provider == "rocketmq":
                assert options.rocketmq is not None
                if os.environ.get("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH") != options.rocketmq.ca_file:
                    raise ValueError(
                        "RocketMQ requires its explicit CA environment before process startup"
                    )
                config = await asyncio.to_thread(read_rocketmq_config, options)
                command_handler, failed_command = receiver.receive_command, receiver.failed_command
                if runtime.transaction_services is not None:
                    services = runtime.transaction_services
                    # Require the final owner/type/revision, including received
                    # topics, before constructing a subscriber or opening Start.
                    for topic, owner in (
                        (COMMANDS, "qs.mysql"),
                        (ACKS, "qs.mysql"),
                        (EVENTS, "ai.mysql"),
                    ):
                        selected = [r for r in config.routes if r.topic == topic]
                        if not selected or any(
                            (r.message_type, r.owner, r.route_revision)
                            != ("TRANSACTION", owner, "m3-1")
                            for r in selected
                        ):
                            raise ValueError("Transaction workflow route binding differs")
                    commands = TransactionCommandReceiver(
                        receiver,
                        admission,
                        services.runtime,
                        services.archives,
                        database_seconds=services.database_seconds,
                    )
                    command_handler, failed_command = (
                        commands.receive_command,
                        commands.failed_command,
                    )
                else:
                    runtime.publishers = {config.endpoint: RocketMQPublisher(config)}
                for topic, handler, failed in (
                    (COMMANDS, command_handler, failed_command),
                    (ACKS, receiver.receive_ack, receiver.failed_ack),
                ):
                    runtime.subscribers.append(
                        RocketMQSubscriber(
                            config,
                            topic,
                            CHANNELS[topic],
                            handler=handler,
                            failed_handler=failed,
                            invalid_handler=receiver.invalid_wire,
                            max_attempts=8,
                        )
                    )
            else:
                runtime.http = AsyncHTTPClient(force_instance=True)
                topology = FailureTopology(options.nsqd, runtime.http, (COMMANDS, ACKS))
                nsq_publishers = {address: NSQPublisher(address) for address in options.nsqd}
                runtime.publishers = dict(nsq_publishers)
                for topic, handler, failed in (
                    (COMMANDS, receiver.receive_command, receiver.failed_command),
                    (ACKS, receiver.receive_ack, receiver.failed_ack),
                ):
                    runtime.subscribers.append(
                        NSQSubscriber(
                            topic,
                            CHANNELS[topic],
                            publishers=nsq_publishers,
                            handler=handler,
                            failed_handler=failed,
                            invalid_handler=receiver.invalid_wire,
                            failure_ready=topology,
                            max_attempts=8,
                            max_in_flight=options.max_in_flight,
                        )
                    )
            runtime.tx = tx
            payload_reader: PayloadReader = store
            if runtime.transaction_services is not None:
                payload_reader = ArchivePayloadReader(runtime.transaction_services.archives, store)
            else:
                runtime.relay = MQRelay(
                    tx, store, runtime.publishers, address=next(iter(runtime.publishers))
                )
            runtime.payloads = MessagePayloads(MySQLPayloadAccess(tx, payload_reader))
            return runtime
        except BaseException:
            await runtime.close()
            raise

    def components(self, state: RuntimeState, drain_seconds: float) -> list[Component]:
        result: list[Component] = []
        if self.transaction_services is not None:
            transactions = self.transaction_services.runtime

            async def run_transactions() -> None:
                await transactions.start()
                # An idle/paused producer still registers all owner topics and
                # serves Checker; this event denotes Start, not write admission.
                self.publisher_ready.set()
                await self._transaction_closed.wait()

            async def halt_transactions() -> None:
                # Seal external claims/subscriptions first. Accepted model work
                # may still need a new terminal half message during its drain.
                await asyncio.gather(
                    *(
                        asyncio.shield(task)
                        for name, task in state.tasks.items()
                        if name not in {"http", "mq_transactions"}
                    ),
                    return_exceptions=True,
                )
                try:
                    await transactions.close(grace_seconds=5)
                except ShutdownIncompleteError:
                    event("transaction_shutdown_incomplete", "mq_transactions")
                    raise
                finally:
                    self._transaction_closed.set()

            result.append(
                Component(
                    "mq_transactions",
                    run_transactions,
                    halt_transactions,
                    self.publisher_ready.is_set,
                    drain_seconds + 6,
                )
            )
        for index, (address, publisher) in enumerate(self.publishers.items()):

            async def run(address: str = address, publisher: Publisher = publisher) -> None:
                await publisher.start()
                self.publishers_started.add(address)
                if len(self.publishers_started) == len(self.publishers):
                    self.publisher_ready.set()
                await publisher.wait()

            async def halt(publisher: Publisher = publisher) -> None:
                # Business drains may still require receipts and failure handoff PUBs.
                await asyncio.gather(
                    *(
                        asyncio.shield(task)
                        for name, task in state.tasks.items()
                        if name != "http" and not name.startswith("mq_publisher_")
                    ),
                    return_exceptions=True,
                )
                await publisher.stop(grace_seconds=5)

            def is_started(address: str = address) -> bool:
                return address in self.publishers_started

            result.append(
                Component(
                    f"mq_publisher_{index}",
                    run,
                    halt,
                    is_started,
                    drain_seconds + 6,
                )
            )
        for index, subscriber in enumerate(self.subscribers):
            started = asyncio.Event()

            async def consume(
                subscriber: Subscriber = subscriber, started: asyncio.Event = started
            ) -> None:
                await self.publisher_ready.wait()
                await subscriber.start()
                started.set()
                await subscriber.wait()

            async def halt_subscriber(subscriber: Subscriber = subscriber) -> None:
                await subscriber.stop(grace_seconds=drain_seconds)

            result.append(
                Component(
                    f"mq_subscriber_{index}",
                    consume,
                    halt_subscriber,
                    started.is_set,
                    drain_seconds + 1,
                )
            )
        return result

    async def step(self) -> int:
        if self.relay is None or not self.publish_enabled or not self.publisher_ready.is_set():
            return 0  # Starting/stopping publisher cannot reject or hold durable results.
        # Historical ownership moves only through an explicitly reviewed handoff.
        # New state events are already staged on their original business transaction.
        await self.relay.step()
        return 0

    async def close(self) -> None:
        if self.transaction_services is not None:
            if self._close_task is None:
                self._close_task = asyncio.create_task(self._close_transactions())
            cancelled = False
            while True:
                try:
                    await asyncio.shield(self._close_task)
                    break
                except asyncio.CancelledError:
                    # Repeated parent cancellation is not proof that a blocking
                    # RPC ended. An actually cancelled close task still raises.
                    if self._close_task.done():
                        await self._close_task
                    cancelled = True
            if cancelled:
                raise asyncio.CancelledError
            return
        await self._close_resources()

    async def _close_transactions(self) -> None:
        assert self.transaction_services is not None
        for subscriber in self.subscribers:
            await subscriber.stop(grace_seconds=0)
        try:
            await self.transaction_services.runtime.close(grace_seconds=0)
        except ShutdownIncompleteError:
            await self.transaction_services.runtime.wait()
        self._transaction_closed.set()
        await self._close_resources()

    async def _close_resources(self) -> None:
        for subscriber in self.subscribers:
            await subscriber.stop(grace_seconds=0)
        for publisher in self.publishers.values():
            await publisher.stop(grace_seconds=0)
        if self.http is not None:
            self.http.close()
        await self.resources.aclose()
        if self.database is not None:
            self.database.state_events = None
