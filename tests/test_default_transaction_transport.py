"""Actual default DI/SDK/transport constructors with a controlled schema read.

No Start, real database, Broker or QS RPC business acceptance is claimed here.
"""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from dishka import Provider, Scope, provide
from reliable_messaging.transport import consumer_group_v2, physical_topic_v2
from sqlalchemy.ext.asyncio import AsyncEngine

from qs_ai.application.interpretation.ports import EvidenceSource
from qs_ai.bootstrap import messaging
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.messaging import MessagingRuntime
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.bootstrap.transaction_services import TransactionServices
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.transaction_archive import ArchivePayloadReader
from qs_ai.infrastructure.persistence.mysql.transaction_receipts import TransactionMessagingStore
from qs_ai.infrastructure.workflow_transport.messaging import ACKS, CHANNELS, COMMANDS
from qs_ai.infrastructure.workflow_transport.transaction_events import TransactionOnlyEvents
from qs_ai.infrastructure.workflow_transport.transaction_receiver import TransactionCommandReceiver
from tests.test_evaluation_container import configured
from tests.test_transaction_services_container import transaction_settings


@pytest.mark.parametrize("paused", [False, True])
def test_actual_broker_profile_configuration_uses_the_host_async_driver(tmp_path, paused):
    from tests.integration.rocketmq_state_host import default_transport_settings

    files = {
        name: str(tmp_path / name) for name in ("ai.sign", "qs.sign", "ai.encrypt", "qs.encrypt")
    }
    value = default_transport_settings(
        "mysql://test:fixture@127.0.0.1:3306/disposable",
        SimpleNamespace(store_id="fixture-primary", writer_epoch="m3-1"),
        {"endpoint": "127.0.0.1:1", "ca_file": str(tmp_path / "ca.crt")},
        files,
        tmp_path / "credentials.json",
        tmp_path / "routes.json",
        paused=paused,
    )
    assert value.database_url.get_secret_value().startswith("mysql+asyncmy://")
    assert value.messaging.publish_enabled is not paused
    assert value.messaging.transactions["ai.mysql"].store_id == "fixture-primary"
    assert value.messaging.transactions["ai.mysql"].writer_epoch == "m3-1"


@pytest.mark.parametrize("route_fault", [False, True])
async def test_default_assembly_borrows_one_app_sender_without_relay_or_legacy_recorder(
    tmp_path, monkeypatch, route_fault
):
    settings = transaction_settings(tmp_path)
    document = json.loads((tmp_path / "routes.json").read_text())
    for row in document["routes"]:
        if row["topic"] in (COMMANDS, ACKS):
            row["subscription"] = CHANNELS[row["topic"]]
            row["consumer_group"] = consumer_group_v2(row["topic"], row["subscription"])
            if route_fault:
                # Canonical SDK route, but the wrong transaction owner.
                row["owner"] = "ai.mysql"
                row["physical_topic"] = physical_topic_v2(
                    row["topic"], "TRANSACTION", "ai.mysql", "m3-1"
                )
    (tmp_path / "routes.json").write_text(json.dumps(document))
    settings = configured(
        messaging=settings.messaging.model_dump(), grpc={"access_address": "qs.invalid:9090"}
    )
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", settings.messaging.rocketmq.ca_file)
    connect = Mock(side_effect=AssertionError("Constructors cannot connect or install schema"))
    monkeypatch.setattr(AsyncEngine, "connect", connect)
    checked, channel_closed = [], []
    db = AsyncMock()

    @asynccontextmanager
    async def open_session(self):
        try:
            yield db
        finally:
            checked.append("rolled_back")

    async def schema(session):
        assert session is db
        checked.append("readonly_schema")

    @asynccontextmanager
    async def channel(*args):
        try:
            yield MagicMock()
        finally:
            channel_closed.append(True)

    class Overrides(Provider):
        @provide(scope=Scope.REQUEST, override=True)
        def source(self) -> EvidenceSource:
            return Mock(spec=EvidenceSource)

    monkeypatch.setattr(Transactions, "open", open_session)
    monkeypatch.setattr(messaging, "require_recording_schema", schema)
    monkeypatch.setattr(messaging, "mtls_channel", channel)
    forbidden = Mock(
        side_effect=AssertionError("Transaction assembly cannot create legacy publishers")
    )
    for name in ("StateEventRecorder", "MQRelay", "RocketMQPublisher", "NSQPublisher"):
        monkeypatch.setattr(messaging, name, forbidden)
    container = create_container(settings, EvaluationProvider(), Overrides())
    value = None
    try:
        host = await container.get(TransactionServices | None)
        database = await container.get(Database)
        if route_fault:
            with pytest.raises(ValueError, match="route binding differs"):
                await MessagingRuntime.create(container, settings, b"", b"", b"")
            assert database.state_events is None and channel_closed == [True]
        else:
            value = await MessagingRuntime.create(container, settings, b"", b"", b"")
            assert value.transaction_services is host
            assert value.tx.database is database
            assert isinstance(database.state_events, TransactionOnlyEvents)
            assert value.relay is None and not value.publishers
            assert len(value.subscribers) == 2
            handlers = [subscriber.handler for subscriber in value.subscribers]
            assert isinstance(handlers[0].__self__, TransactionCommandReceiver)
            assert isinstance(handlers[1].__self__.store, TransactionMessagingStore)
            assert isinstance(value.payloads.access.store, ArchivePayloadReader)
            assert not host.runtime.stats()["ready"]
        assert checked == ["readonly_schema", "rolled_back"]
        connect.assert_not_called()
        forbidden.assert_not_called()
    finally:
        if value is not None:
            await value.close()
        await container.close()
    assert channel_closed == [True]
