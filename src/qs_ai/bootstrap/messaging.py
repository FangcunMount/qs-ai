"""Explicit embedded MQ assembly on the unified application's loop and database."""

import asyncio
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from dishka import AsyncContainer
from jwcrypto import jwk  # type: ignore[import-untyped]
from reliable_messaging.nsq import NSQPublisher, NSQSubscriber
from reliable_messaging.protected import TrustedSigner
from tornado.httpclient import AsyncHTTPClient

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.evaluation.execution_mode import EvaluationRuntimeLimits
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.governance.solution_models import EditableModelPolicy
from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.bootstrap.lifecycle import Component, RuntimeState
from qs_ai.config import Settings
from qs_ai.contracts.workflow import messaging_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.command_admission import WorkflowCommandAdmission
from qs_ai.infrastructure.workflow_transport.messaging import ACKS, CHANNELS, COMMANDS
from qs_ai.infrastructure.workflow_transport.mq_failure import FailureTopology
from qs_ai.infrastructure.workflow_transport.mq_receiver import CommandReceiver
from qs_ai.infrastructure.workflow_transport.mq_relay import MQRelay
from qs_ai.infrastructure.workflow_transport.payloads import PayloadResolver
from qs_ai.infrastructure.workflow_transport.state_events import StateEventRecorder
from qs_ai.transport.grpc.message_payloads import MessagePayloads


def read_key(path: str, *, private: bool, expected_id: str | None = None) -> Any:
    # Never put path contents, JOSE errors, or private material in diagnostics.
    try:
        with Path(path).open("rb") as stream:
            data = stream.read(65537)
        if len(data) > 65536:
            raise ValueError
        key = jwk.JWK.from_json(data.decode())
        if not key.get("kid") or (expected_id is not None and key.get("kid") != expected_id):
            raise ValueError
        if private != key.has_private:
            raise ValueError
        if key.get("kty") != "EC" or key.get("crv") != "P-256":
            raise ValueError
        return key
    except Exception:
        raise ValueError("Invalid configured messaging key") from None


class MessagingRuntime:
    recorder: StateEventRecorder
    tx: Transactions
    relay: MQRelay
    payloads: MessagePayloads

    def __init__(self) -> None:
        self.resources = AsyncExitStack()
        self.publishers: dict[str, NSQPublisher] = {}
        self.subscribers: list[NSQSubscriber] = []
        self.publishers_started: set[str] = set()
        self.publisher_ready = asyncio.Event()
        self.database: Database | None = None
        self.http: AsyncHTTPClient | None = None

    @classmethod
    async def create(
        cls, container: AsyncContainer, settings: Settings, ca: bytes, cert: bytes, key: bytes
    ) -> "MessagingRuntime":
        runtime = cls()
        try:
            options = settings.messaging
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
            runtime.recorder = StateEventRecorder(store, signing, recipient)
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
            runtime.http = AsyncHTTPClient(force_instance=True)
            topology = FailureTopology(options.nsqd, runtime.http, (COMMANDS, ACKS))
            runtime.publishers = {address: NSQPublisher(address) for address in options.nsqd}
            for topic, handler, failed in (
                (COMMANDS, receiver.receive_command, receiver.failed_command),
                (ACKS, receiver.receive_ack, receiver.failed_ack),
            ):
                runtime.subscribers.append(
                    NSQSubscriber(
                        topic,
                        CHANNELS[topic],
                        publishers=runtime.publishers,
                        handler=handler,
                        failed_handler=failed,
                        invalid_handler=receiver.invalid_wire,
                        failure_ready=topology,
                        max_attempts=8,
                        max_in_flight=options.max_in_flight,
                    )
                )
            runtime.tx = tx
            runtime.relay = MQRelay(tx, store, runtime.publishers, address=next(iter(options.nsqd)))
            runtime.payloads = MessagePayloads(tx, store)
            return runtime
        except BaseException:
            await runtime.close()
            raise

    def components(self, state: RuntimeState, drain_seconds: float) -> list[Component]:
        result: list[Component] = []
        for index, (address, publisher) in enumerate(self.publishers.items()):

            async def run(address: str = address, publisher: NSQPublisher = publisher) -> None:
                await publisher.start()
                self.publishers_started.add(address)
                if len(self.publishers_started) == len(self.publishers):
                    self.publisher_ready.set()
                await publisher.wait()

            async def halt(publisher: NSQPublisher = publisher) -> None:
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
                subscriber: NSQSubscriber = subscriber, started: asyncio.Event = started
            ) -> None:
                await self.publisher_ready.wait()
                await subscriber.start()
                started.set()
                await subscriber.wait()

            async def halt_subscriber(subscriber: NSQSubscriber = subscriber) -> None:
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
        if not self.publisher_ready.is_set():
            return 0  # Starting/stopping publisher cannot reject or hold durable results.
        # Bounded handoff of original durable rows. No manufactured historical Run events.
        async with self.tx.open() as db:
            await db.begin()
            count = await self.recorder.handoff(db)
            await db.commit()
        await self.relay.step()
        return count

    async def close(self) -> None:
        for subscriber in self.subscribers:
            await subscriber.stop(grace_seconds=0)
        for publisher in self.publishers.values():
            await publisher.stop(grace_seconds=0)
        if self.http is not None:
            self.http.close()
        await self.resources.aclose()
        if self.database is not None:
            self.database.state_events = None
