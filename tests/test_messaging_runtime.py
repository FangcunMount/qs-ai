"""Owned lifecycle checks with fake transport; no Broker or model network calls."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from jwcrypto import jwk
from pydantic import ValidationError
from sqlalchemy.exc import ProgrammingError

from qs_ai.bootstrap import messaging
from qs_ai.bootstrap.lifecycle import RuntimeState, supervise
from qs_ai.bootstrap.messaging import MessagingRuntime, read_key
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql import database as database_module
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.messaging_observations import KINDS


def test_messaging_disabled_by_default_and_enabled_requires_explicit_secrets():
    assert not Settings().messaging.enabled
    with pytest.raises(ValidationError):
        Settings(messaging={"enabled": True})


def test_keys_require_private_public_roles_id_and_bounded_file(tmp_path):
    key = jwk.JWK.generate(kty="EC", crv="P-256", kid="test.key")
    path = tmp_path / "key.json"
    path.write_text(key.export_private())
    assert read_key(str(path), private=True, expected_id="test.key").has_private
    for private, kid in [(False, "test.key"), (True, "wrong")]:
        with pytest.raises(ValueError, match="Invalid configured messaging key"):
            read_key(str(path), private=private, expected_id=kid)
    path.write_text(key.export_public())
    assert not read_key(str(path), private=False).has_private
    path.write_text("x" * 65537)
    with pytest.raises(ValueError, match="Invalid configured messaging key"):
        read_key(str(path), private=True)


class Transport:
    def __init__(self, name, events, fail=False):
        self.name, self.events, self.fail = name, events, fail
        self.closed = asyncio.Event()

    async def start(self):
        self.events.append(self.name + ":start")
        if self.fail:
            raise RuntimeError("startup failure")

    async def wait(self):
        await self.closed.wait()

    async def stop(self, **kwargs):
        self.events.append(self.name + ":stop")
        self.closed.set()


@pytest.mark.parametrize("fail", [False, True])
async def test_publisher_starts_before_subscribers_and_stops_after_their_drain(fail):
    events = []
    runtime = MessagingRuntime()
    runtime.publishers = {"a": Transport("pub", events, fail=fail)}
    runtime.subscribers = [Transport("commands", events), Transport("acks", events)]
    stop, state = asyncio.Event(), RuntimeState()
    task = asyncio.create_task(supervise(runtime.components(state, 0.1), state, stop))
    if fail:
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(task, 2)
        assert not state.ready and state.failed
    else:
        async with asyncio.timeout(2):
            while not state.ready:  # noqa: ASYNC110 - observe supervisor readiness
                await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 2)
        assert events[0] == "pub:start"
        assert events.index("pub:stop") > events.index("commands:stop")
        assert events.index("pub:stop") > events.index("acks:stop")
        assert not state.ready
    await runtime.close()


async def test_partial_assembly_failure_closes_owned_clients():
    runtime = MessagingRuntime()
    runtime.publishers = {"a": AsyncMock()}
    runtime.subscribers = [AsyncMock()]
    runtime.http = type("HTTP", (), {"close": lambda self: None})()
    await runtime.close()
    runtime.subscribers[0].stop.assert_awaited_once_with(grace_seconds=0)
    runtime.publishers["a"].stop.assert_awaited_once_with(grace_seconds=0)


async def test_relay_cannot_publish_or_handoff_before_publishers_ready():
    runtime = MessagingRuntime()
    runtime.tx = AsyncMock()
    runtime.relay = AsyncMock()
    assert await runtime.step() == 0
    runtime.tx.open.assert_not_called()
    runtime.relay.step.assert_not_awaited()


@pytest.mark.parametrize("schema", ["missing", "incomplete", "complete", "canceled"])
async def test_recording_preflight_precedes_transport_and_borrows_shared_pool(monkeypatch, schema):
    # Exercise the real Transactions lifecycle and schema reader without a database/network.
    # The peripheral integration suite separately covers the actual MySQL schema and rows.
    database = Database(None)
    database.engine = AsyncMock()
    session = AsyncMock()
    session.info = {}
    result = MagicMock()
    kinds = KINDS[:-1] if schema == "incomplete" else KINDS
    result.mappings.return_value.all.return_value = [
        {"kind": kind, "recorded_count": 0, "since_epoch_seconds": 1} for kind in kinds
    ]
    session.execute.return_value = result
    if schema == "missing":
        session.execute.side_effect = ProgrammingError(
            "unredacted SQL", {}, Exception("unredacted database error")
        )
    elif schema == "canceled":
        session.execute.side_effect = asyncio.CancelledError
    factory = MagicMock()
    factory.return_value.__aenter__.return_value = session
    sessionmaker = MagicMock(return_value=factory)
    monkeypatch.setattr(database_module, "async_sessionmaker", sessionmaker)
    container = MagicMock()
    container.get = AsyncMock(return_value=database)
    scope = container.return_value.__aenter__.return_value
    scope.get = AsyncMock(return_value=object())
    monkeypatch.setattr(messaging, "read_key", lambda *args, **kwargs: object())
    for name in (
        "TrustedSigner",
        "StateEventRecorder",
        "WorkflowCommandAdmission",
        "PayloadResolver",
        "CommandReceiver",
        "AsyncHTTPClient",
        "FailureTopology",
        "MQRelay",
        "MessagePayloads",
    ):
        monkeypatch.setattr(messaging, name, MagicMock())

    channel_closed = []

    @asynccontextmanager
    async def channel(*args):
        try:
            yield MagicMock()
        finally:
            channel_closed.append(True)

    channel_factory = MagicMock(side_effect=channel)
    monkeypatch.setattr(messaging, "mtls_channel", channel_factory)

    def transport():
        # No transport may be constructed before the preflight read has been rolled back.
        session.rollback.assert_awaited_once()
        factory.return_value.__aexit__.assert_awaited_once()
        return AsyncMock()

    publisher = MagicMock(side_effect=lambda *args, **kwargs: transport())
    subscriber = MagicMock(side_effect=lambda *args, **kwargs: transport())
    monkeypatch.setattr(messaging, "NSQPublisher", publisher)
    monkeypatch.setattr(messaging, "NSQSubscriber", subscriber)
    settings = Settings(
        grpc={"access_address": "qs.invalid:9090"},
        messaging={
            "enabled": True,
            "nsqd": {"nsq.invalid:4150": "http://nsq.invalid:4151"},
            "signing_key_file": "/unused/sign.jwk",
            "decrypt_key_files": {"ai.encrypt": "/unused/decrypt.jwk"},
            "qs_signer_files": {"qs.sign": "/unused/qs-sign.jwk"},
            "qs_recipient_key_file": "/unused/qs-encrypt.jwk",
        },
    )
    if schema == "complete":
        runtime = await MessagingRuntime.create(container, settings, b"", b"", b"")
        publisher.assert_called_once()
        assert subscriber.call_count == 2
        for client in [*runtime.publishers.values(), *runtime.subscribers]:
            client.start.assert_not_awaited()
        assert database.state_events is runtime.recorder
        await runtime.close()
        assert channel_closed == [True]
    else:
        error = asyncio.CancelledError if schema == "canceled" else RuntimeError
        with pytest.raises(error) as failure:
            await MessagingRuntime.create(container, settings, b"", b"", b"")
        if schema != "canceled":
            assert str(failure.value) == "MQ required technical observation schema unavailable"
        publisher.assert_not_called()
        subscriber.assert_not_called()
        channel_factory.assert_not_called()
        messaging.StateEventRecorder.assert_not_called()
    container.get.assert_awaited_once_with(Database)
    sessionmaker.assert_called_once()
    assert sessionmaker.call_args.args == (database.engine,)
    session.begin.assert_awaited_once()
    session.commit.assert_not_awaited()
    session.rollback.assert_awaited_once()
    factory.return_value.__aexit__.assert_awaited_once()
    assert database.state_events is None
    database.engine.dispose.assert_not_awaited()


async def test_disabled_messaging_never_checks_recording_schema(monkeypatch):
    require = AsyncMock()
    monkeypatch.setattr(messaging, "require_recording_schema", require)
    container = MagicMock()
    container.get = AsyncMock()
    with pytest.raises(ValueError, match="MQ requires"):
        await MessagingRuntime.create(container, Settings(), b"", b"", b"")
    require.assert_not_awaited()
    container.get.assert_not_awaited()
